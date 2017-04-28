use std::collections::BTreeMap;
use std::cmp::{PartialEq, PartialOrd, Ordering};

use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom, Cursor, Error, ErrorKind};
use std::path::PathBuf;
use std::fs::{File, OpenOptions, remove_file, rename};
use std::string::FromUtf8Error;
extern crate byteorder;
use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

extern crate tempfile;
use self::tempfile::tempfile;
use dedup::chunk::{Chunk, CHUNK_HEADER_LEN, MAX_CHUNKS_LEN};

const METADATA_BUFF_SIZE :usize= 10 * 1024;//use to load .index
pub const DATA_BUFF_SIZE :usize= 10 * 1024;//use to load .index


pub const HEADER_LEN_ENTRY:usize = 64 + 8 +8 +8;
pub struct BlobEntry{
    pub id      : String,
    pub _ref    : u64,
    pub pos     : u64,
    pub len     : u64,
}
    
impl BlobEntry{
    /* pub fn from(cursor : &mut Cursor<&[u8]>)->Result<BlobEntry, FromUtf8Error>{
        Ok(BlobEntry{
            id     : try!(String::from_utf8(
            Vec::from(&(*cursor.get_ref())[cursor.position() as usize 
                      ..cursor.position() as usize+64]))),
            _ref   : {cursor.seek(SeekFrom::Current(64));cursor.read_u64::<LittleEndian>().unwrap()}, 
             pos   : cursor.read_u64::<LittleEndian>().unwrap(), 
            len    : cursor.read_u64::<LittleEndian>().unwrap(),
        })
    }*/
pub fn from(cursor : &mut BufReader<File>)->Result<BlobEntry, Error>{
        Ok(BlobEntry{
            id     :{
                let mut buff =  String::with_capacity(64);
                let mut handle = cursor.take(64);
                if try!(handle.read_to_string(&mut buff)) != 64{
                    return Err(Error::new(ErrorKind::Interrupted, "Can not read chunk id"));
                }
                buff 
            },
            _ref   : cursor.read_u64::<LittleEndian>().unwrap(), 
             pos   : cursor.read_u64::<LittleEndian>().unwrap(), 
            len    : cursor.read_u64::<LittleEndian>().unwrap(),
        })
    }
}


//ID du block, <= id min des chunks à l'intérieur du blob
pub struct Blob{
    pub id          : String,
    n_chunks    : usize,
    
    pub loaded      : bool,
    pub mutated     : bool,
    pub metadata    : Vec<BlobEntry>,//Only file after a load, clean after a save
}

impl Blob{
    pub fn new(id : &str) -> Blob{
        Blob{
            id       : String::from(id),   
            n_chunks : 0,

            loaded   : false,
            mutated  : false,
            metadata : Vec::new(),
        }
    }
    //Number of chunks stored in this block
    pub fn len(&self) -> usize{
        self.n_chunks
    }
    
    pub fn location_data(&self, mut location:PathBuf) -> PathBuf{
        location.push(String::from("blob_")+&self.id);
        location.set_extension("data");
        location
    }
    
    pub fn location_tmpdata(&self, mut location:PathBuf) -> PathBuf{
        location.push(String::from("blob_")+&self.id);
        location.set_extension("data.tmp");
        location
    }

    pub fn location_index(&self, mut location:PathBuf) -> PathBuf{
        location.push(String::from("blob_")+&self.id);
        location.set_extension("index");
        location
    }

    fn find_pos(&self, id:&String) -> usize{
        if self.metadata.is_empty(){ 
            return 0;
        }

        let mut beg = 0;
        let mut end = self.metadata.len()-1;

        while beg+1 < end{
            let mid = (beg + end) / 2;

            if self.metadata[mid].id < *id{
                beg = mid;
            }else if self.metadata[mid].id == *id{
                return mid;
            }else{
                end = mid;
            }
        }
        
        if self.metadata[beg].id < *id{
            end
        }else{
            beg 
        }
    }

    pub fn exists(&self, id:&String) -> bool {
        assert!(self.loaded);
        if self.metadata.is_empty(){
            false
        }else{    
            let meta_pos = self.find_pos(&id);
            self.metadata[meta_pos].id == *id
        }
    }

    pub fn add(&mut self, location:PathBuf, chunk:&Chunk) -> Result<(), Error>{
        //les chunks sont ordonées par id dans l'index (et dans le .data pas trié )
        assert!(self.loaded);
        let mut meta_pos = self.find_pos(&chunk.id);
        if !self.exists(&chunk.id){
            //Data management
            let location = self.location_data(location);
            let mut file = OpenOptions::new().create(true).append(true).open(location)?;
            let mut writer = BufWriter::with_capacity(DATA_BUFF_SIZE, file);
                       
            let pos = try!{writer.seek( SeekFrom::End(0) )};
            try!{chunk.write(&mut writer)};
           

            meta_pos += if meta_pos < self.len() && self.metadata[meta_pos].id < chunk.id {1}else{0};
            if meta_pos < self.len(){
            self.metadata.insert(meta_pos, BlobEntry{
                id : chunk.id.clone(),
                _ref : 1,
                pos : pos,
                len : chunk.len + CHUNK_HEADER_LEN as u64
            });
            }else{
                self.metadata.push( BlobEntry{
                id : chunk.id.clone(),
                _ref : 1,
                pos : pos,
                len : chunk.len + CHUNK_HEADER_LEN as u64
            });
            }
           
            self.n_chunks += 1;
        }else{
            let ref mut entry = self.metadata[meta_pos];
            entry._ref += 1;
        }

        self.mutated = true;
        Ok(())
    }
    
    pub fn remove(&mut self, dir:PathBuf, id:&String) -> Result<(), Error>{
        assert!(self.loaded);

        if !self.exists(id){
            return Ok(());
        }

        let pos = self.find_pos(id);
        {
            let ref mut entry = self.metadata[pos];
            entry._ref -= 1;
        };

        if {self.metadata[pos]._ref} !=0{
            return Ok(());
        }

        //.data
        try!(rename(self.location_data(dir.clone()),
            self.location_tmpdata(dir.clone())));
        let mut reader = BufReader::with_capacity(DATA_BUFF_SIZE, 
                            File::open(self.location_tmpdata(dir.clone()))?);
        let mut writer = BufWriter::with_capacity(DATA_BUFF_SIZE, 
                          File::create(self.location_data(dir.clone()))?);
        
        let mut read = 0;
        loop{
            let len ={
                let buff = try!(reader.fill_buf());
                
                let start = self.metadata[pos].pos;
                let stop = self.metadata[pos].pos+self.metadata[pos].len;
                let len = buff.len() as u64;

                if read < start && read+len > start{
                    try!(writer.write_all(&buff[..(start-read) as usize]));
                    try!(writer.write_all(&buff[(len - (read+len).saturating_sub(stop)) as usize..]));
                }else if start <= read && read + len  <= stop{
                    //Nothing   
                }else if read < stop && read+len > stop{
                    try!(writer.write_all(&buff[..start.saturating_sub(read) as usize]));
                    try!(writer.write_all(&buff[(stop-read) as usize..]));
                }else{
                    try!(writer.write_all(&buff[..]));
                }

                buff.len()
            };
            if len == 0{break;}

            read += len as u64;
            reader.consume(len);
        }
        let max = try!(writer.seek(SeekFrom::End(0)));
        try!(remove_file(self.location_tmpdata(dir.clone())));

        let len = self.metadata[pos].len;
        let begin = self.metadata[pos].pos;
        for j in 0..self.metadata.len(){
            let tmp  =self.metadata.len();
            let ref mut entry = self.metadata[j];
            if {entry.pos} > begin {
                entry.pos -= len;
            }
        }

        self.metadata.remove(pos);
        self.n_chunks -= 1;
        self.mutated = true;

        Ok(())
}

    pub fn get(&self, location:PathBuf, id:&String) -> Result<Option<Chunk>, Error>{
        assert!(self.loaded);

        if !self.exists(id){
            Ok(None)
        }else{
            let meta_pos = self.find_pos(id);
            let ref entry=self.metadata[meta_pos];
            let location = self.location_data(location);

            let mut buffer = File::open(location)?;
            let mut reader = BufReader::with_capacity(DATA_BUFF_SIZE, buffer);
            try!( reader.seek( SeekFrom::Start(entry.pos) ) );
            Ok(Some(try!(Chunk::from(&mut reader))))
        }
    }
    
    //self contains chunks where chunk.id<next.id(middle)
    pub fn split(&mut self, dir:PathBuf)-> Result<Blob, Error>{
        assert!(self.loaded);

            let last_n = self.n_chunks; 
            
            let next_metadata = self.metadata.split_off(self.n_chunks>>1);
            self.n_chunks = self.n_chunks >> 1;
            let mut next = Blob{
                id : next_metadata.first().unwrap().id.clone(),
                n_chunks : last_n-self.n_chunks,

                metadata : next_metadata,
                loaded : true,
                mutated : true
            };
            
            
            let _location       = self.location_data(dir.clone());
            {
                let mut tmpfile:File = try!(File::create(self.location_tmpdata(dir.clone())));
                let mut pos = 0;
                let mut i = 0;//current chunk in self.meta
                let mut next_pos = 0;
                let mut j = 0;//current chunk in next_meta

                let mut file : File = try!(File::open(_location.clone()));
                let mut reader      = BufReader::with_capacity(DATA_BUFF_SIZE, file);

                let mut writer      = BufWriter::with_capacity(DATA_BUFF_SIZE, &tmpfile);
                let next_location    = next.location_data(dir.clone()); 
                let mut next_file   = try!(File::create(next_location));
                let mut next_writer = BufWriter::with_capacity(DATA_BUFF_SIZE, next_file);   
                
                while i+j < last_n{
                    let chunk = try!(Chunk::from(&mut reader));
                    if chunk.id < next.id{
                        try!(chunk.write(&mut writer));
                       
                        let tmp = self.find_pos(&chunk.id);
                        let entry = &mut self.metadata[tmp];
                        entry.pos = pos; 
                        pos += entry.len;
                        i+=1;
                    }else{
                        try!(chunk.write(&mut next_writer));
                        
                        let tmp = next.find_pos(&chunk.id);
                        let entry = &mut next.metadata[tmp];
                        entry.pos = next_pos; 
                        next_pos += entry.len;
                        j+=1;

                    }
                }
            }

            //Copy tempfile
            try!(remove_file(self.location_data(dir.clone())));
            try!(rename(self.location_tmpdata(dir.clone()), self.location_data(dir.clone())));
            self.mutated = true;

            Ok(next)
    }

    pub fn load(&mut self, dir:PathBuf) -> Result<(), Error>{
        if self.loaded{
            return Ok(());
        }

        let location = self.location_index(dir);
        let mut file = match File::open(location.clone()){
            Ok(file) => file,
            Err(err) => 
                if err.kind() == ErrorKind::NotFound && self.n_chunks == 0{
                    self.loaded = true;
                    return Ok(());
                }else{
                    return Err(err);
                }
        };
        let mut reader = BufReader::with_capacity(METADATA_BUFF_SIZE, file);
        
        //Format .index
        //---------------------------------
        //| n_chunks as u64 | chunk_entries ...
        //---------------------------------
        //where chunk entry
        //----------------------------------------------------------
        //| id chunk 64 bytes| _ref as u64| pos as u64 | data length as u64|
        //----------------------------------------------------------
        assert!(self.metadata.is_empty());
       
        self.n_chunks = reader.read_u64::<LittleEndian>()? as usize;
        self.metadata = Vec::with_capacity(self.n_chunks);

        for _ in 0..self.n_chunks{
            self.metadata.push( try!(BlobEntry::from(&mut reader)));
        }

        self.loaded = true;
        Ok(())
    }
    
    pub fn store(&mut self, dir:PathBuf) -> Result<(), Error>{
        if !self.loaded || !self.mutated{
            return Ok(());
        }
        
        if self.n_chunks == 0{
            return self.delete_files(dir);
        }
        let location = self.location_index(dir);
        let mut file = try!(File::create(location));
        file.set_len(8 + HEADER_LEN_ENTRY as u64* self.metadata.len() as u64);
        let mut writer = BufWriter::with_capacity(METADATA_BUFF_SIZE, file);
        
        assert!(self.metadata.len() == self.n_chunks);
        writer.write_u64::<LittleEndian>(self.n_chunks as u64)?;
        for entry in &self.metadata{
            assert!(entry.id.as_bytes().len() == 64);
            writer.write_all(entry.id.as_bytes())?;
            writer.write_u64::<LittleEndian>(entry._ref)?;
            writer.write_u64::<LittleEndian>(entry.pos)?;
            writer.write_u64::<LittleEndian>(entry.len)?;
        }

        self.mutated = false;
        Ok(())
    }

    pub fn save(&mut self, dir:PathBuf) -> Result<(), Error>{
        if !self.loaded || !self.mutated{
            return Ok(());
        }
        if self.n_chunks == 0{
            return self.delete_files(dir);
        }
        let location = self.location_index(dir);
        let mut file = try!(File::create(location));
        file.set_len(8 + HEADER_LEN_ENTRY as u64* self.metadata.len() as u64);
        let mut writer = BufWriter::with_capacity(METADATA_BUFF_SIZE, file);
        
        assert!(self.metadata.len() == self.n_chunks);
        writer.write_u64::<LittleEndian>(self.n_chunks as u64)?;
        for entry in &self.metadata{
            assert!(entry.id.as_bytes().len() == 64);
            writer.write_all(entry.id.as_bytes())?;
            writer.write_u64::<LittleEndian>(entry._ref)?;
            writer.write_u64::<LittleEndian>(entry.pos)?;
            writer.write_u64::<LittleEndian>(entry.len)?;
        }

        self.metadata.clear();
        self.loaded = false;
        self.mutated = false;

        Ok(()) 
    }

    pub fn delete_files(&self, dir:PathBuf) -> Result<(), Error>{
        let r1 = match remove_file(self.location_index(dir.clone())){
            Ok(_) => Ok(()),
            Err(ref err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
        };
       
        match remove_file(self.location_data(dir.clone())){
            Ok(_)=>r1, 
            Err(ref err) if err.kind() == ErrorKind::NotFound => r1,
            err => err
        }
    }

    pub fn check(&self){
        assert!(self.loaded);

        for entry in &self.metadata{
            assert!(entry.id >= self.id);      
        }
    }
}

impl PartialEq for Blob{
    fn eq(&self, other: &Blob)->bool{
        self.id == other.id
    }
}

impl PartialOrd for Blob{
    fn partial_cmp(&self, other:&Blob) -> Option<Ordering>{
        Some(self.id.cmp(&other.id))
    }
}

/* TODO write and  test
 *
pub struct BlobManager{
    blobs : Vec<Blob> //sorted
        //TODO replace Vec with BTree
}

impl BlobManager{
    pub fn new() -> BlobManager{
        BlobManager{
            blobs : vec![Blob::new("")]
        }
    }
    
    fn find_pos(&self, id:&String) -> usize{
        let mut beg = 0;
        let mut end = self.blobs.len()-1;

        while beg != end{
            let mid = (beg + end) / 2;

            if self.blobs[mid].id < *id{
                beg = mid;
            }else if self.blobs[mid].id == *id{
                return mid;
            }else{
                end = mid;
            }
        }

        beg
    }

    fn find_block(&mut self, id:&String) -> &mut Blob{
        assert!(!self.blobs.is_empty());
        let pos = self.find_pos(id);
        &mut self.blobs[pos]
    }
    
    fn insert(&mut self, blob:Blob){
        let pos = self.find_pos(&blob.id);
        self.blobs.insert(pos, blob);
    }

    pub fn add(&mut self, chunk:ChunkMetadata){
        let new_blob ={
            let blob = self.find_block(&chunk.id);

            blob.add(chunk);
            blob.split()
        };

        match new_blob{ 
            None => (),
            Some(b2)=>{
                self.insert(b2)
            }
        }
    }

    pub fn exists(&mut self, id_chunk:&String)->bool{
        let pos = self.find_pos(id_chunk);
        self.blobs[pos].exists(id_chunk)
    }
}
*/

