use std::io::prelude::*;
use std::io::{Cursor, Error, BufReader, BufWriter, SeekFrom};
use std::fs::File;

use dedup::chunk::{HChunk, Chunk, ChunkType, ChunkEntry, ENTRY_LEN, MAX_CHUNKS_LEN};
use dedup::rabin::{RabinHasher, BUFF_MAX_SIZE};
use storage::storage_handler::BlobIndex;
use dedup::dedup::{Phi,Rule};

use std::collections::HashMap;

use std::cmp::min;

//TODO
//LA fonction de hashage pour les id devrait être encodé dans un module
//Pareil pour rabin, utiliser le même module
extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;

pub struct Client{
    db : BlobIndex,
    phi  : Phi,      
}

fn sha256(location:&str) -> Result<String, Error>{
    let mut hasher = Sha256::new();
    let mut reader = BufReader::with_capacity(BUFF_MAX_SIZE, 
                                              try!(File::open(location))); 
    loop{
        let len = {
            let buff = try!(reader.fill_buf());
            hasher.input(&buff[0..buff.len()]);
            buff.len()
        };
        if len == 0{
            break;    
        }
        reader.consume(len);    
    }
        
    Ok(hasher.result_str()) 
}


impl Client{
    pub fn new(db_location:String) -> Client {
        Client{
            db : BlobIndex::new(db_location, 1000, 100),
            phi : Phi::new()        
        }
    }
    
    fn _get(&mut self, mut entries: Vec<ChunkEntry>, ref out_location:&str) -> Result<u64, Error>{
        let mut writer = BufWriter::with_capacity(BUFF_MAX_SIZE, 
            try!(File::create(out_location)));
        
        let mut memory = HashMap::new();
        let mut effectively = 0;
        while !entries.is_empty(){
            
        print!("Entries {} \n", entries.len());
            let entry = entries.pop().unwrap();
            if memory.contains_key(&entry.id){
                panic!("FUCK fUCK FUCK!!!\n");
            };
            memory.insert(entry.id.clone(), 1);
            let chunk = match try!(self.db.get_chunk(&entry.id)){
                None => panic!("No chunk {} in db\n", entry.id),
                Some(chunk) => chunk
            };
            
            match chunk._type{
                ChunkType::Data =>{
                    effectively += chunk.len;
                    try!(writer.seek(SeekFrom::Start(chunk.begin)));
                    try!(writer.write_all(&chunk.data[..]));
                }, ChunkType::Index =>{
                    for _entry in &try!(chunk.to_entries()){
                        print!("{}\n", chunk.id);
                            assert!(_entry.id != chunk.id);
                    }
                    entries.extend( try!(chunk.to_entries()) );
                }, ChunkType::File => panic!("DB corrupted, it could not be file")
            }
        }
        print!("Effectively {}\n", effectively);
        Ok( try!(writer.seek(SeekFrom::End(0))) )
    }

    pub fn get(&mut self, id:String, out_location : &str) -> Result<(),Error>{
        let file_chunk = match try!(self.db.get_chunk(&id)){
            None => panic!("No file {} in db\n", id),
            Some(chunk) =>chunk
        };
        let file_size = try!(self._get(try!(file_chunk.to_entries()), out_location));
        print!("Coucou\n");
        //Integrity check, TODO maybe automated in some mod
        let mut hasher = Sha256::new();
        let mut reader = BufReader::with_capacity(BUFF_MAX_SIZE, 
                                                  try!(File::open(out_location))); 
        loop{
            let len = {
                let buff = try!(reader.fill_buf());
                hasher.input(&buff[0..buff.len()]);
                buff.len()
            };
            if len == 0{
                break;    
            }
            reader.consume(len);    
        }
        
        if hasher.result_str() != id{
            panic!("File corrupted\n");
        }
        Ok(())    
    }
    

    //TODO dedup les index s'ils sont trop gros, notament celui du fichier, pour l'instant rien
    //n'est fait
    fn _set(&mut self, chunk:&mut Chunk, reader:&mut BufReader<File>, hasher:&mut RabinHasher, masks:&Vec<u64>, i:usize) -> Result<(),Error>{
        if try!(self.db.exists_chunk(&chunk.id)){
            return self.db.add_chunk(&chunk);
            //TODO panic!("il faut reflechier à la supression, sur un block data on retiens que le nimbre d'index qui pointe dessus");
        }
       
        match chunk._type{
            ChunkType::Data =>{
                reader.seek( SeekFrom::Start(chunk.begin) );
                chunk.read_data( reader);
                self.db.add_chunk(&chunk);
            },
            _ =>{
                print!("in {} {}\n", chunk.begin, chunk.len);
                let mut subchunks = Vec::new();
                try!(reader.seek(SeekFrom::Start(chunk.begin)));
                try!(hasher.process(masks[i], &mut subchunks, reader, chunk.begin, chunk.end()));
                
                let mut index = Cursor::new(Vec::with_capacity(subchunks.len()*ENTRY_LEN));
                let mut j=0;
                for mut subchunk in &mut subchunks{
                    j+=subchunk.len;
                    if i == masks.len()-1{
                        subchunk._type = ChunkType::Data;
                    }else{
                        subchunk._type = ChunkType::Index;
                    }
                    subchunk.write_entry(&mut index);
                    
                    try!(self._set(&mut subchunk, reader, hasher, masks, i+1));
                }
                print!("J {}\n", j);
                
                
                if( subchunks.len()>1){ 
                    chunk.data = index.into_inner();
                    chunk.len = chunk.data.len() as u64;
                    self.db.add_chunk(&chunk);
                }else{//subchunk is chunk and already saved
                     assert!(chunk.id == subchunks[0].id);
                }

                print!("out {}\n",i);
            }
        }

        Ok(())
        
    }

    pub fn set(&mut self, input_location : &str) -> Result<String,Error>{
        let rule = self.phi.hasher(input_location);
        let mut hasher = RabinHasher::new(rule.min_size, rule.max_size);
        
        let mut reader = BufReader::with_capacity( BUFF_MAX_SIZE, 
                           try!(File::open(input_location)));
        let file_size ={
            let tmp = try!(reader.seek(SeekFrom::End(0)));
            try!(reader.seek(SeekFrom::Start(0)));
            tmp
        };
        let file_id =  try!(sha256(input_location));
        let mut root = Chunk{
            _type : ChunkType::File,
            id    : file_id.clone(),
            begin : 0,
            len   : file_size,
            data  : Vec::new(),
        };
        self._set(&mut root, &mut reader, &mut hasher, &rule.masks, 0);

        assert!(try!(self.db.exists_chunk(&root.id)));
        print!("END OF SET\n");
        Ok(root.id.clone())    
    }

}



