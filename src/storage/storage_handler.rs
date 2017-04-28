use std::collections::{BTreeMap, HashMap};
use std::collections::vec_deque::VecDeque;
use std::cmp::{PartialEq, PartialOrd, Ordering};

use std::io::prelude::*;
use std::io::Error;
use std::path::PathBuf;

extern crate time;
use self::time::precise_time_s;


use dedup::chunk::{Chunk};
use storage::blob::{Blob};


const STORING_DELAY : f64 = 5.; //Second
//For now, we will use a sorted vect, after it should be implemented with BTree
//Maybe mark blob as corrupted if a remove / add failed?
//REwrite so that no inconsistency state
pub struct BlobIndex{
    db_dir : String,
    max_chunks_in_blobs : usize,
    max_blobs_loaded : usize,

    blobs : Vec<Blob>,

    cache_index : HashMap<String, f64>, //id => last save
    cache_fifo  : VecDeque<String>
}

impl BlobIndex{
    pub fn new(db_dir:String, max_chunks_in_blobs:usize, max_blobs_loaded:usize) -> BlobIndex{
        BlobIndex{
            db_dir  : db_dir,
            max_chunks_in_blobs : max_chunks_in_blobs,
            max_blobs_loaded : max_blobs_loaded,
            blobs   : vec![Blob::new("")],
            
            cache_index : HashMap::with_capacity(max_blobs_loaded),
            cache_fifo  : VecDeque::with_capacity(max_blobs_loaded),
        }
    }
   
    pub fn len(&self) -> usize{
        self.blobs.len()
    }

    pub fn cache_len(&self) -> usize{
        self.cache_index.len()
    }
    
    fn load_pos(&mut self, pos : usize) -> Result<(),Error>{
        let id = self.blobs[pos].id.clone(); 
        if self.cache_index.contains_key(&id){
            Ok(())
        }else{
            if self.cache_fifo.len() >= self.max_blobs_loaded{
                match self.cache_fifo.pop_front(){
                    None => panic!("MAX_BLOBS_LOADED must be strictly positive"),
                    Some(last_id) =>{
                        self.cache_index.remove(&last_id);
                        let last_pos = self.find_pos(&last_id);
                        try!(self.blobs[last_pos].save(PathBuf::from(&self.db_dir)));
                    }
                }
            }

            try!(self.blobs[pos].load(PathBuf::from(&self.db_dir)));
            self.cache_fifo.push_back(id.clone());
            self.cache_index.insert(id, precise_time_s());
            
            Ok(())
        }
    }

    fn store_pos(&mut self, pos : usize) -> Result<(),Error>{
        let id = self.blobs[pos].id.clone();
        let last_save = self.cache_index[&id];
        
        if precise_time_s() - last_save >= STORING_DELAY{
            try!(self.blobs[pos].store(PathBuf::from(&self.db_dir)));
            self.cache_index.insert(id, precise_time_s()); 
        }

        Ok(())
    }
    
    fn find_pos(&self, id:&String) -> usize{
        if self.blobs.is_empty(){ 
            return 0;
        }

        let mut beg = 0;
        let mut end = self.blobs.len()-1;

        while beg+1 < end{
            let mid = (beg + end) / 2;

            if self.blobs[mid].id < *id{
                beg = mid;
            }else if self.blobs[mid].id == *id{
                return mid;
            }else{
                end = mid;
            }
        }
        
        if self.blobs[end].id > *id{
            beg
        }else{
            end     
        }
    }


    pub fn add_chunk(&mut self, chunk : &Chunk) -> Result<(), Error>{
        let pos = self.find_pos(&chunk.id);
        try!(self.load_pos(pos));

        match self.blobs[pos].add( PathBuf::from(&self.db_dir), chunk){
            Ok(()) if self.blobs[pos].len() > self.max_chunks_in_blobs=>{
                match self.blobs[pos].split(PathBuf::from(&self.db_dir)){
                    Ok(mut next) =>{
                        self.blobs[pos].check();
                        next.check();
                        let res = next.save(PathBuf::from(&self.db_dir));
                        self.blobs.insert(pos+1, next);
                        match self.store_pos(pos) {
                            Ok(())=> res,
                            Err(err) => match res {
                                Ok(()) => Err(err),
                                Err(err1) => Err(err1)
                            }
                        }
                    },
                    Err(err1)  =>{
                        let _ =self.store_pos(pos);
                        Err(err1)
                    }
                }
            }
            res =>{self.blobs[pos].check();
                match self.store_pos(pos){
                    Ok(()) => res,
                    Err(err) => match res{
                        Ok(()) => Err(err),
                        _   => res
                    }
                }
            }
        }
    }

    pub fn exists_chunk(&mut self, id : &String) -> Result<bool, Error>{
        let pos = self.find_pos(id);
        try!(self.load_pos(pos));

        let exists = self.blobs[pos].exists(id); 

        try!(self.store_pos(pos));
        Ok(exists)
    }

    pub fn get_chunk(&mut self, id : &String) -> Result<Option<Chunk>, Error>{
        let pos = self.find_pos(id);
        try!(self.load_pos(pos));

        let res = self.blobs[pos].get(PathBuf::from(&self.db_dir), 
                                             id); 

        match self.store_pos(pos){
            Ok( _ ) => res,
            Err(err) => match res{
                Ok(_) => Err(err),
                _     => res  
            }
        }

    }

    pub fn remove_chunk(&mut self, id : &String) -> Result<(), Error>{
        let pos = self.find_pos(id);
        try!(self.load_pos(pos));

        let res = self.blobs[pos].remove(PathBuf::from(&self.db_dir), 
                                             id); 

        match self.store_pos(pos){
            Ok( _ ) => res,
            Err(err) => match res{
                Ok(_) => Err(err),
                _     => res  
            }
        }
    }
}

impl Drop for BlobIndex{
    fn drop(&mut self){
        for blob in &mut self.blobs{
            blob.save(PathBuf::from(&self.db_dir));
        }
    }
}

