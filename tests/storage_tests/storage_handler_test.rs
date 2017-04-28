use std::collections::{BTreeMap, HashMap};
use std::collections::vec_deque::VecDeque;
use std::cmp::{PartialEq, PartialOrd, Ordering};

use std::io::prelude::*;
use std::io::{Error, BufReader, SeekFrom};
use std::fs::File;
use std::path::PathBuf;

extern crate time;
use self::time::precise_time_s;


extern crate t;
use self::t::dedup::rabin::{RabinHasher};
use self::t::dedup::chunk::{Chunk};
use self::t::storage::storage_handler::BlobIndex;
use self::t::storage::blob::DATA_BUFF_SIZE;

const FILE_LOCATION : &'static str = "data/txt/11-0.txt";
const DIR_LOCATION_ADD  : &'static str = "tests/storage_tests/data/storage_handler/";
const DIR_LOCATION_REMOVE :&'static str = "tests/storage_tests/data/storage_handler/remove";
//ASSERT THAT THERE IS ONLY ONE MASK INVOLVED IN THE HIERARCHY CONSTRUCTION,
//much simpler to build chunk vect for now
//should be extend later
fn chunks() -> Vec<Chunk>{
    let file_location = PathBuf::from(FILE_LOCATION);

    let mut hasher = RabinHasher::new(vec![0b1111111111], 256, 4096);//1Ko
    //Check .data integrity
    let mut reader = BufReader::with_capacity( DATA_BUFF_SIZE, 
                            File::open(file_location).unwrap());

           let mut hierarchy = Vec::new();
    let _ = hasher.process(&mut hierarchy, &FILE_LOCATION).unwrap();
   
    let mut v = Vec::with_capacity(hierarchy.len());
    for hchunk in hierarchy{
        v.push(hchunk.as_chunk());
    }

    for chunk in &mut v{
        reader.seek( SeekFrom::Start(chunk.begin) ).unwrap();
        chunk.read_data(&mut reader).unwrap();
    }
    v
}

#[test]
fn unittest_add(){
    let mut chunks = chunks();   //151 chunks
    let mut blobIndex = BlobIndex::new(String::from(DIR_LOCATION_ADD), 10, 10);

    for chunk in &chunks{
        blobIndex.add_chunk(chunk).unwrap();
        assert!( blobIndex.exists_chunk(&chunk.id).unwrap() );
    }
    
    for chunk in &chunks{ 
        assert!( blobIndex.exists_chunk(&chunk.id).unwrap() );

        let tmp = blobIndex.get_chunk(&chunk.id).unwrap().unwrap();

        assert_eq!( tmp.id, chunk.id);
        assert_eq!( tmp.begin, chunk.begin);
        assert_eq!( tmp.len, chunk.len);
        assert_eq!( tmp.data[..], chunk.data[..]);
    }
}

#[test]
fn unittest_remove(){
    let mut chunks = chunks();   //151 chunks
    let mut blobIndex = BlobIndex::new(String::from(DIR_LOCATION_REMOVE), 1000, 10);

    for chunk in &chunks{
        blobIndex.add_chunk(chunk).unwrap();
        assert!( blobIndex.exists_chunk(&chunk.id).unwrap() );
    }
    
    for chunk in &chunks{
        blobIndex.remove_chunk(&chunk.id).unwrap();
        assert!( !blobIndex.exists_chunk(&chunk.id).unwrap() );
    }
}

