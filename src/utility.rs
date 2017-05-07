use std::io::prelude::*;
use std::io::{Cursor, Error, BufReader, BufWriter, SeekFrom};
use std::fs::{self, DirEntry,File};
use std::path::{Path,PathBuf};

use dedup::rabin::{BUFF_MAX_SIZE};
extern crate crc64;

extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;

pub fn visit_files(dir:&Path,  f:&Fn(&DirEntry)) -> Result<(),Error>{
    if dir.is_dir(){
        for entry in fs::read_dir(dir)?{
            let entry = entry ?;
            let path = entry.path();
            if path.is_dir(){
                visit_files(&path, f)?;
            }else{
                f(&entry);
            }
        }
    }

    Ok(())
}

//TODO
//LA fonction de hashage pour les id devrait être encodé dans un module
//Pareil pour rabin, utiliser le même module

// TODO rendre la suite tolérante aux intéruptuosn
pub fn sha256(location:&str) -> Result<String, Error>{
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

pub fn crc64(location:&str) -> Result<u64, Error>{
    let mut crc = 0;
    let mut reader = BufReader::with_capacity(BUFF_MAX_SIZE, 
                                              try!(File::open(location))); 
    loop{
        let len = {
            let buff = try!(reader.fill_buf());
            crc = crc64::crc64(crc, &buff[0..buff.len()]);
            buff.len()
        };
        if len == 0{
            break;    
        }
        reader.consume(len);    
    }
        
    Ok(crc) 
}

