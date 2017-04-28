extern crate t;
use std::path::PathBuf;
use std::fs::{File,metadata};
use std::io::{BufReader,SeekFrom};
use std::io::prelude::*;

use self::t::dedup::rabin::{RabinHasher};
use self::t::dedup::chunk::{Chunk, CHUNK_HEADER_LEN};
use self::t::storage::blob::{Blob, DATA_BUFF_SIZE, HEADER_LEN_ENTRY};

const FILE_LOCATION : &'static str = "data/txt/11-0.txt";
const DIR_LOCATION  : &'static str = "tests/storage_tests/data/blobs/";


//ASSERT THAT THERE IS ONLY ONE MASK INVOLVED IN THE HIERARCHY CONSTRUCTION,
//much simpler to build chunk vect for now
//should be extend later
fn chunks() -> Vec<Chunk>{
    let mut hasher = RabinHasher::new(vec![0b1111111111], 256, 4096);//1Ko
    
    let mut hierarchy = Vec::new();
    let _ = hasher.process(&mut hierarchy, &FILE_LOCATION).unwrap();
   
    let mut v = Vec::with_capacity(hierarchy.len());
    for hchunk in hierarchy{
        v.push(hchunk.as_chunk());
    }
    v
}

//Check metadata and their representation on disks
fn check_blob(chunks:&mut Vec<Chunk>, blob : &Blob){
    let file_location = PathBuf::from(FILE_LOCATION);
    let blob_location = PathBuf::from(DIR_LOCATION);

    //Check that entries are increasingly sorted
    assert_eq!(chunks.len(), blob.len());
    for i in 1..blob.metadata.len(){
        assert!(blob.metadata[i-1].id < blob.metadata[i].id);
    }
    
    for chunk in chunks.iter(){
         assert!( blob.exists(&chunk.id));
    }

    chunks.sort_by_key(|ref c| c.id.clone());
    for (mut chunk, entry) in chunks.into_iter().zip(blob.metadata.iter()){
        //Check intergrity of metadata
            assert_eq!(chunk.id, entry.id);
            assert_eq!(chunk.len, entry.len - CHUNK_HEADER_LEN as u64);

        //Check .data integrity
        let mut reader = BufReader::with_capacity( DATA_BUFF_SIZE, 
                            File::open(file_location.clone()).unwrap());

        reader.seek( SeekFrom::Start(chunk.begin) ).unwrap();
        chunk.read_data(&mut reader).unwrap();
        let chunk2 = blob.get(blob_location.clone(), &chunk.id).unwrap().unwrap();

        assert_eq!(chunk2.id, chunk.id);
        assert_eq!(chunk2.begin, chunk.begin);
        assert_eq!(chunk2.len, chunk.len);
        
        assert_eq!(chunk.data.len(), chunk.len as usize);
        assert_eq!(chunk2.data.len(), chunk.len as usize);
        assert_eq!(chunk.data[..], chunk2.data[..]);
    }
}

//Make blob, save it and reload it
fn blob(prefix:&str) -> Blob{
    let file_location = PathBuf::from(FILE_LOCATION);
    let blob_location = PathBuf::from(DIR_LOCATION);
    let mut chunks = chunks(); 

    let mut blob = Blob::new(prefix);
    blob.delete_files(blob_location.clone()).unwrap();
    blob.load(blob_location.clone()).unwrap();

    let mut reader = BufReader::with_capacity( DATA_BUFF_SIZE, 
                               File::open(file_location.clone()).unwrap());
    for chunk in &mut chunks{
        reader.seek( SeekFrom::Start(chunk.begin) ).unwrap();
        chunk.read_data(&mut reader).unwrap();
        
        blob.add(blob_location.clone(), &chunk).unwrap();
    };
       
    blob.save(blob_location.clone()).unwrap();
    blob.load(blob_location.clone()).unwrap();
    blob 
}
  


//this test ensure that
//* entry are increasingly sorted( according to their id)
//* adding twice the same chunk will not increase the .data or metadata
//* entry pos and len are consistent
//* incrementing _ref when adding more than once time a chunk
//* right number of bytes stored in .index and .data
#[test]
fn unittest_add(){
    let file_location = PathBuf::from(FILE_LOCATION);
    let blob_location = PathBuf::from(DIR_LOCATION);
    let mut chunks = chunks(); 

    let mut blob = blob("add");           
    check_blob(&mut chunks, &blob);

    //Adding chunks more thant once and checks related
        let mut sum = 0; 
        for chunk in &mut chunks{
            assert!( blob.exists(&chunk.id));
            {
                let mut reader = BufReader::with_capacity( DATA_BUFF_SIZE, 
                                   File::open(file_location.clone()).unwrap());
                reader.seek( SeekFrom::Start(chunk.begin) ).unwrap();
                chunk.read_data(&mut reader).unwrap();    
            }
            blob.add(blob_location.clone(), &chunk).unwrap();
            blob.add(blob_location.clone(), &chunk).unwrap();

            sum += chunk.len;
        };
        
        
        check_blob(&mut chunks, &blob);

        for entry in &blob.metadata{
            assert_eq!(entry._ref, 3);
        }
    
    assert!(blob.mutated);    
    blob.save(blob_location.clone()).unwrap();
    assert!(blob.metadata.is_empty());
    assert!(!blob.mutated && !blob.loaded);

    //Check data on drive
    let m_data = metadata(blob.location_data(blob_location.clone())).unwrap();
    assert_eq!(m_data.len(), chunks.len() as u64 * CHUNK_HEADER_LEN as u64 + sum); 
    let m_index = metadata(blob.location_index(blob_location.clone())).unwrap();
    assert_eq!(m_index.len(), 8 + chunks.len() as u64* HEADER_LEN_ENTRY as u64); 
}

//
//* get test
#[test]
fn unittest_get(){
    let mut chunks = chunks();
    let blob = blob("get");
    check_blob(&mut chunks, &blob);
}

//split
#[test]
fn unittest_split(){
    let blob_location = PathBuf::from(DIR_LOCATION);

    let mut chunks = chunks();
    let mut blob1 = blob("");
    check_blob(&mut chunks, &blob1);
    
    let blob2 = blob1.split(blob_location).unwrap();
    let limit = chunks.len()/2;
    assert_eq!(blob1.len(), limit);
    assert_eq!(blob2.len(), chunks.len()-limit);
    
    chunks.sort_by_key(|ref c| c.id.clone());
    let mut chunks2 = chunks.split_off(limit);
    blob1.check();
    blob2.check();
    check_blob(&mut chunks, &blob1);
    check_blob(&mut chunks2, &blob2);
}

#[test]
fn unittest_remove(){
    let blob_location = PathBuf::from(DIR_LOCATION);
    let mut chunks = chunks(); 

    let mut blob = blob("remove");           
    check_blob(&mut chunks, &blob);

   
    //Long test because it is in average in  O(n^2)
    loop{
        match chunks.pop(){
            None => break,
            Some(chunk) =>{
                blob.remove(blob_location.clone(), &chunk.id).unwrap();
                //Try deleting a missing chunk
                blob.remove(blob_location.clone(), &chunk.id).unwrap();

                blob.save(blob_location.clone()).unwrap();
                blob.load(blob_location.clone()).unwrap();
                check_blob(&mut chunks, &blob);
            }
        }
    }
}

