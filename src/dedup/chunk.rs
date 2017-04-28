use std::collections::vec_deque::VecDeque;

use std::io::prelude::*;
use std::io::{Cursor, Error, BufReader, BufWriter, SeekFrom, ErrorKind};
use std::fs::{File,metadata};
use std::cmp::min;
use std::string:: FromUtf8Error;

extern crate byteorder;
use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
//use byteorder::{LittleEndian, ReadBytesExt, ReadBytesExt};

extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;

//CHeck that id is 64bytes
const HEADER_LEN:u64 = 8 + 64 + 8;
pub const HEADER_LEN_ENTRY:u64 = 64 + 8 + 8;
//Describe of the content describe by the chunk
//* Data : it is a part of a data file 
//* Index : it is an index of chunks : ie "list" of chunks' entries
#[derive(Copy, Clone)]
pub enum ChunkType{
    Data,
    Index,
    File,
}

pub const ENTRY_LEN:usize = 64 + 8 + 8;
pub struct ChunkEntry{ //In index data
    pub id      : String,
    pub begin     : u64, //TODO blbo entry should use begin to instead of pos
    pub len     : u64,
}

impl ChunkEntry{
    pub fn from<T:Read>(cursor:&mut T)-> Result<ChunkEntry, Error>{
        Ok(ChunkEntry{
            id     :{
                let mut buff =  String::with_capacity(64);
                let mut handle = cursor.take(64);
                if try!(handle.read_to_string(&mut buff)) != 64{
                    return Err(Error::new(ErrorKind::Interrupted, "Can not read chunk id"));
                }
                buff 
            },
            begin  : cursor.read_u64::<LittleEndian>().unwrap(), 
            len    : cursor.read_u64::<LittleEndian>().unwrap(),
        })
    }
}
pub const MAX_CHUNKS_LEN :usize = 1 << 20;//TODO to incorporate into rabin

impl ChunkType{
    pub fn from(x:u8) ->ChunkType{
            if x ==0 { ChunkType::Data}
            else if x == 1{
                ChunkType::Index
            }else{
                ChunkType::File
            }
    }
    
    pub fn to(&self) -> u8{
        match *self {
            ChunkType::Data => 0,
            ChunkType::Index => 1,
            ChunkType::File => 2,
        }
    }
}

//Describe a block of a file
pub struct Chunk{
    pub _type : ChunkType,  //type of the chunk
    pub id : String,        //see the report
    pub begin : u64,        //position of the begining of the block, in the file, of data described by the chunk
    pub len : u64,          //length of the block          

    pub data : Vec<u8>
}

pub const CHUNK_HEADER_LEN :usize= 1 + 2 * 8 +64;
impl Chunk{
    //The position, in the file, after the last byte of the block, 
    //described by the chunk,
    pub fn end(&self) -> u64{
        self.len + self.begin
    }
    
    //without data
    pub fn from<T:Read+BufRead >(cursor : &mut T)->Result<Chunk, Error>{
        let mut chunk = Chunk{
            _type  : ChunkType::from(
                cursor.read_u8().unwrap()),
            id     :{
                let mut buff =  String::with_capacity(64);
                let mut handle = cursor.take(64);
                if try!(handle.read_to_string(&mut buff)) != 64{
                    return Err(Error::new(ErrorKind::Interrupted, "Can not read chunk id"));
                }
                buff 
            },
            begin  : cursor.read_u64::<LittleEndian>().unwrap(), 
            len    : cursor.read_u64::<LittleEndian>().unwrap(),
            data   : Vec::new(),
        };
        chunk.read_data(cursor); 
        Ok(chunk)
    }

    pub fn write<T:Write>(&self, writer:&mut T) -> Result<(), Error>{
        writer.write_u8(self._type.to())?;
        writer.write_all(self.id.as_bytes())?; //64
        writer.write_u64::<LittleEndian>(self.begin)?;
        writer.write_u64::<LittleEndian>(self.len)?;
        writer.write_all(&self.data[..])
    }

    pub fn read_data<T:Read+BufRead>(&mut self, reader:&mut T) -> Result<(), Error>{
        if self.len as usize > self.data.len(){
            let additional = self.len as usize - self.data.len();
            self.data.reserve_exact( additional );
        }else{
            self.data.truncate( self.len as usize);
        }
        unsafe { self.data.set_len(self.len as usize) };
        reader.read_exact(&mut self.data );
        Ok(())
    }
    
    //Same function than hchunk, need to get ride of a convertion, TODO duplication really needed
    pub fn write_entry<T:Write>(&self, writer:&mut T) -> Result<(), Error>{
        writer.write_all(self.id.as_bytes())?; //64
        writer.write_u64::<LittleEndian>(self.begin)?;
        writer.write_u64::<LittleEndian>(self.len)
    }
    
    pub fn to_entries(&self) -> Result<Vec<ChunkEntry>, Error>{
        match self._type{
            ChunkType::Index|ChunkType::File =>{
                assert_eq!(self.data.len()% ENTRY_LEN, 0);

                let mut entries = Vec::with_capacity(self.data.len() / ENTRY_LEN);
                for r_entry in self.data.chunks(ENTRY_LEN){
                    entries.push(  try!(ChunkEntry::from(&mut Cursor::new(r_entry))) );
                }

                Ok(entries)
            },
            _=>panic!("Can not be applied to such chunk")
        }
    }
    
    pub fn split(&mut self, max_size : u64) -> Vec<Chunk>{
        match self._type{
            ChunkType::Data=>(),
            _=>panic!("Spliting non data chunk not implemented yet")
        }

        let mut chunks = Vec::new();

        if self.len >= max_size{
            let mut beg = self.begin;

            while beg < self.end(){
                let len = min(max_size, self.end()-beg);
                let mut new = Chunk{
                    _type   : self._type,
                    id      : String::from(""),
                    begin   : beg,
                    len     : len,
                    data    : Vec::new()
                };
                
                chunks.push(new);
                beg += len;
            }
        }
        
        return chunks
    }
    
}


//Hierarchycal Chunk
//If subchunks is empty then it is Chunk of data
//Otherwith it is an index chunk
pub struct HChunk{
    pub id : String,
    pub begin : u64,
    pub len : u64,
    pub mask : usize,//nthe number of the mask

    pub subchunks : VecDeque<HChunk>,
    pub lvl : usize,
}


impl HChunk{
    pub fn new(begin :u64, mask:usize, lvl : usize) -> HChunk{
        HChunk{ 
            id      : String::from(""),
            begin   : begin,
            len     : 0,
            mask    : mask,
            subchunks: VecDeque::new(),
            lvl     : lvl,
        }
    }
   
    //Same function than chunk, need to get ride of a convertion, TODO duplication really needed
    pub fn write_entry<T:Read + Write>(&self, writer:&mut T) -> Result<(), Error>{
        writer.write_all(self.id.as_bytes())?; //64
        writer.write_u64::<LittleEndian>(self.begin)?;
        writer.write_u64::<LittleEndian>(self.len)
    }

    //We loose the subchunks
    pub fn as_chunk(&self) -> Chunk{
        Chunk{
            _type   : if !self.subchunks.is_empty(){ 
                ChunkType::Index 
            }else{ 
                ChunkType::Data },
            id      : self.id.clone(),
            begin   : self.begin,
            len     : self.len,
            data    : Vec::new(),
        }
    }

    //Determines if the related chunk is a Data chunk
    fn is_data(&self) -> bool{
        self.subchunks.is_empty()
    }

    //Determines if the related chunk is a Index chunk
    fn is_index(&self) -> bool(){
        !self.subchunks.is_empty()
    }
    
    //The position, in the file, after the last byte of the block, 
    //described by the chunk,
    pub fn end(&self) -> u64{
        self.len + self.begin
    }

    //Determines the type of the related chunk
    pub fn _type(&self) -> ChunkType{
        if self.is_data(){
            return ChunkType::Data;
        }else{
            return ChunkType::Index;
        }

    }
    
    //Add an hchunk to subchunks
    pub fn add_subchunk(&mut self, chunk:HChunk){
        self.subchunks.push_back(chunk);
    }
    
    //Add a vector of hchunk to subchunks
    pub fn add_subchunks(&mut self, chunks:Vec<HChunk>){
        self.subchunks.extend(chunks);
    }

    fn subchunks_from(&mut self, begin:u64, end:u64, chunks:&mut VecDeque<HChunk>){ 
        while !self.subchunks.is_empty(){
            let mut chunk:Option<HChunk> = self.subchunks.pop_front();
            
            match chunk{
                None => break,
                Some(chunk) =>{ 
                    if begin <= chunk.begin && chunk.end() < end{
                        chunks.push_back(chunk);
                    }else if chunk.end() >= end{
                        break;
                    }
                }
            }
        }
    }

    //(,out of bounds)
    pub fn split(&mut self, max_size : u64) -> (Vec<HChunk>,usize){
        let mut out_of_bounds = 0;
        let mut chunks = Vec::new();

        if self.len >= max_size{
            let mut beg = self.begin;

            while beg < self.end(){
                let len = min(max_size, self.end()-beg);
                let mut new = HChunk{
                    id      : String::from(""),
                    begin   : beg,
                    len     : len,
                    mask    : self.mask, 
                    lvl     : self.lvl,
                    subchunks: VecDeque::new(),
                };
                self.subchunks_from(new.begin, new.end(), &mut new.subchunks);
                
                chunks.push(new);
                beg += len;
                out_of_bounds += 1;
            }
        }
        
        while !self.subchunks.is_empty(){
            let chunk = self.subchunks.pop_front().unwrap();
            chunk.lvl.saturating_sub(1);
            chunks.push(chunk);
        }

        return (chunks,out_of_bounds)
    }

    //The size of the chunk when it will be stored
    pub fn size(&self) -> u64{
        HEADER_LEN + if self.is_data(){
            self.len    
        }else{
            self.subchunks.len()as u64 * HEADER_LEN_ENTRY 
        }
    }
    
    pub fn update_id(&mut self, reader:&mut BufReader<File>, hasher:&mut Sha256) -> Result<(), Error>{
        if self.id == ""{
            hasher.reset();
            {
                try!{reader.seek( SeekFrom::Start(self.begin) )};
            }

            let mut len = 0;
            while len < self.len{
                let tmp ={
                    let buff = try!(reader.fill_buf());
                    let tmp = min((self.len-len) as usize, buff.len());

                    hasher.input(&buff[0..tmp]);
                    len+=tmp as u64;
                    
                    if tmp == 0{//TODO
                        panic!("TODO I/O error");
                    }

                    tmp

                };
                reader.consume(tmp);
            }
            
            self.id = hasher.result_str();
        }

        for chunk in &mut self.subchunks{
            chunk.update_id(reader, hasher);
        }
        Ok(())
    }
    
    pub fn clean(&mut self){
        let mut to_delete = Vec::new();
        for (i,hchunk) in self.subchunks.iter().enumerate(){
            if hchunk.begin == self.begin && hchunk.len == hchunk.len{
                to_delete.push(i);
            }
        }
        
        let n = to_delete.len().saturating_sub(1);
        for i in 0..to_delete.len(){
            let _ =self.subchunks.remove(to_delete[n-i]);
        }
        
    }

    pub fn to_linear(&self) -> Vec<HChunk>{
        let mut linear = Vec::new();
        linear.push(HChunk{
                    id      : self.id.clone(),
                    begin   : self.begin,
                    len     : self.len,
                    mask    : self.mask, 
                    lvl     : self.lvl,
                    subchunks: VecDeque::new(),
                });

        for chunk in &self.subchunks{
            linear.extend( chunk.to_linear() );
        }
        linear
    }
  
    //TODO replace as_chunk by this one
    pub fn as_index_chunk(&self) -> Chunk{
        let mut chunk = self.as_chunk();
        
        if !self.subchunks.is_empty(){
            let mut index = Cursor::new(Vec::new());


            for hchunk in &self.subchunks{
                assert!(hchunk.id != self.id);
                hchunk.write_entry(&mut index).unwrap();//if failed here => Ram error
            }
            
            chunk.begin = 0;
            chunk.data = index.into_inner();
            chunk.len = chunk.data.len() as u64;
        }
        
        chunk
    }

    //Build index if needed
    pub fn to_chunks(&self) -> Vec<Chunk>{
        let mut chunks = Vec::new();
        chunks.push(self.as_chunk());
        
        if !self.subchunks.is_empty(){
            let mut index = Cursor::new(Vec::new());


            for hchunk in &self.subchunks{
                assert!(hchunk.id != self.id);
                hchunk.write_entry(&mut index).unwrap();//if failed here => Ram error
                chunks.extend( hchunk.to_chunks() );
            }
            
            chunks[0].begin = 0;
            chunks[0].len = chunks[0].data.len() as u64;
            chunks[0].data = index.into_inner();
        }
        chunks
    }
}

impl PartialEq for HChunk{
    fn eq(&self, other:&HChunk) -> bool{
        if self.id != other.id || self.begin != other.begin || 
            self.len != other.len || self.lvl != other.lvl{
            return false;
        }

        if other.subchunks.len() != self.subchunks.len(){
            return false;
        }

        for (a,b) in self.subchunks.iter().zip( other.subchunks.iter()){
            if a != b{
                return false;
            }
        }

        true 
    }
}


