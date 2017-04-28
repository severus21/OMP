use std::io::Error;
use std::fs::{File,metadata};
use std::io::prelude::*;

use std::path::Path;
use std::io::{BufReader, SeekFrom};
use std::cmp::min;

extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;
use std::vec;

extern crate t;
use self::t::dedup::rabin::{RabinHasher, BUFF_MAX_SIZE};

use self::t::dedup::chunk::HChunk;

//TODO add some test : compute chunks of a large number of file and see if average
//length is good
fn assert_eq_hierarchy(h1:&Vec<HChunk>, h2:&Vec<HChunk>){
    assert_eq!(h1.len(), h2.len());
    for i in 0..h1.len(){
        assert!(h1[i]== h2[i]);
    }
}

//The goal of this test is to try the following behaviour of RabinHasher
//* That processing file1 then file2 is the same that processing file2 then file1   
//*
//
fn aux_files_relationship(masks :&[u64], min_size:u64, max_size:u64){
    let location1 = "data/txt/11-0.txt";
    let location2 = "data/txt/pg10657.txt";

    let mut rb_hasher1 = RabinHasher::new(Vec::from(masks), min_size, max_size);
    let mut rb_hasher2 = RabinHasher::new(Vec::from(masks), min_size, max_size);
    let mut rb_hasher3 = RabinHasher::new(Vec::from(masks), min_size, max_size);

    let mut hierarchy2 = Vec::new();
    let mut hierarchy3 = Vec::new();

    rb_hasher2.process(&mut hierarchy2,location1);
    rb_hasher3.process(&mut hierarchy3, location2);
    
    {
        let mut hierarchy1 = Vec::new();
        rb_hasher1.process(&mut hierarchy1, location1);
        assert_eq_hierarchy(&hierarchy2, &hierarchy1 );
    }
    {
        let mut hierarchy1 = Vec::new();
        rb_hasher1.process(&mut hierarchy1,location2);
        assert_eq_hierarchy(&hierarchy3, &hierarchy1);
    }
}

#[test]
fn test_files_relationship_without_spliting_aggregating(){
   aux_files_relationship(vec![0b11111101, 0b111111011].as_slice(), 0, 1<<63);
}
#[test]
fn test_files_relationship_spliting_without_aggregating(){
   aux_files_relationship(vec![0b111111111111].as_slice() , 0, 1<<10); //average : 4Ko
}
#[test]
fn test_files_relationship_aggregating_without_spliting(){
   aux_files_relationship(vec![0b11111111].as_slice() , 1<<12, 1<<63); //average : 256B
}
#[test]
fn test_files_relationship_spliting_aggregating(){
   aux_files_relationship(vec![0b1111111111, 0b11111111011 ].as_slice() , 1<<9, 1<<11); //average : 1KB
}




//let chunk1 --- chunkn the serie of chunks made by a mask
//Notation chunk_i=(pos_i,len_i,hash_i)
//we will check that
// pos_1-len_1 == 0
// pos_(i+1) == pos_i + len_(i+1), len>0
// pos_n == filesize
// hash_i = sha256( filesize[pos_i-len_i..pos_i])
fn aux_linear_consitancy(masks:&[u64], min_size:u64, max_size:u64) -> Result<(),Error>{
    let location = "data/txt/11-0.txt";
    let sha256sum = String::from("49a0b2726606e1290ac03a63978fa1dd1bd38a8d805704d98265f393533ea094");
    let mut hasher = Sha256::new();
    
    ///IO handling
    let path = Path::new(location);
    let file = File::open(&path).unwrap();
    let metadata = metadata(location).unwrap();
    let mut reader = BufReader::with_capacity(
        min(BUFF_MAX_SIZE as u64, metadata.len()) as usize, file);



    let mut rb_hasher = RabinHasher::new(Vec::from(masks), min_size, max_size);
    let mut hierarchy = Vec::new();
    let hash_file = rb_hasher.process(&mut hierarchy, location).unwrap();

    let mut chunks = Vec::new();
    for _ in 0..masks.len(){
        chunks.push(Vec::new());
    }
    for hchunk in hierarchy{
        for chunk in hchunk.to_linear(){
            chunks[chunk.mask].push(chunk);
        }
    }
    
    for (mask,linear) in chunks.into_iter().enumerate(){
        hasher.reset();
        assert_eq!(hash_file, sha256sum);
        
        //N.B : there is no subchunk because there is only one mask
        let mut last = HChunk::new(0,mask,0);

        assert!(!linear.is_empty());
        for chunk in linear{
            assert_eq!(last.end(), chunk.begin);
            assert_eq!(chunk.end(), chunk.begin+chunk.len);
            
            reader.seek(SeekFrom::Start(chunk.begin));
            let mut len = 0;
            while len < chunk.len{
                let tmp ={
                    let buff = try!(reader.fill_buf());
                    let tmp = min((chunk.len-len) as usize, buff.len());

                    hasher.input(&buff[0..tmp]);
                    len+=tmp as u64;
                    
                    if tmp == 0{//TODO
                        panic!("TODO I/O error");
                    }

                    tmp

                };
                reader.consume(tmp);
            }
        print!("{} {} \n", last.end(), metadata.len());
            assert_eq!(hasher.result_str(), chunk.id);
            hasher.reset();


            last = chunk;  
        }
        print!("{} {} \n", last.end(), metadata.len());
        assert_eq!(last.end(), metadata.len());
    }
    Ok(())
}

#[test]
fn test_linear_consistancy_without_spliting_aggregating(){
    aux_linear_consitancy(vec![0b11111101].as_slice() , 0, 1<<63);
}
#[test]
fn test_linear_consistancy_spliting_without_aggregating(){
    aux_linear_consitancy(vec![0b111111111111].as_slice() , 0, 1<<10); //average : 4Ko
}
#[test]
fn test_linear_consistancy_aggregating_without_spliting(){
    aux_linear_consitancy(vec![0b11111111].as_slice() , 1<<12, 1<<63); //average : 256B
}
#[test]
fn test_linear_consistancy_spliting_aggregating(){
    aux_linear_consitancy(vec![0b1111111111].as_slice() , 1<<9, 1<<11); //average : 1Ko
}

#[test]
fn test_hierarchy_consistancy_without_spliting_aggregating(){
    aux_linear_consitancy(vec![0b11111111, 0b1111111111].as_slice() , 0, 1<<63);
}

#[test]
fn test_hierarchy_consistancy_spliting_without_aggregating(){
    aux_linear_consitancy(vec![0b111111111111,0b11111111111111].as_slice() , 0, 1<<10); //average : 4Ko 8Ko
}
#[test]
fn test_hierarchy_consistancy_aggregating_without_spliting(){
    aux_linear_consitancy(vec![0b11111111,0b1111111111 ].as_slice() , 1<<12, 1<<63); //average : 256B 1024B
}
#[test]
fn test_hierarchy_consistancy_spliting_aggregating(){
    aux_linear_consitancy(vec![0b11111111,0b1111111111, 0b111111111111].as_slice() , 1<<9, 1<<11); //average : 256B 1Ko 4Ko
}


//TODO Average chunk size test, and variances

