use std::fs::{File,metadata};
use std::io::prelude::*;

use std::path::Path;
use std::io::{Error, BufReader, SeekFrom};
use std::cmp::min;

use std::mem::size_of;

use std::ops::Add;

use std::fmt::{Write as FmtWrite};
extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;

extern crate crc64;
extern crate time;
use self::time::precise_time_s;


use dedup::chunk::{Chunk, ChunkType};

//TODO a lot of u64 must become usize( according to do std type for the same)

//TODO
//   etc...
//Log bench(should be desactivated with macro, or function warper 
//Test IO failure
//3)Write test
//4)Optimize algo not code detail
//5)Factorize
//6)Write a good doc
//7)Write the report section related to rabin
//8)Optimize code details for release, not for debug
pub const BUFF_MAX_SIZE:usize = 1024 * 1024 * 64;//16Mo, variable buff size depending on the file size
const WINDOW_LEN:usize = 8;//should be a u64  
const POLYNOMIAL:u64 = (1<<48) + (1<<5) + (1<<3) + (1<<2) + 1;//should be of degree <<(64-8), 48 is a good one;
const WORD_LEN: u32 = 4; //len in subword, for now a subword is a u8 and a word a u64
const SUBWORD_SIZE:u32 = 8; //size in bits, for now it is a u8

#[derive(Copy)]
pub struct Stats{
    avg : f64,
    var : f64,
    th_avg : f64,
    th_var : f64,
    number : usize,
}

impl Stats{
    pub fn new() -> Stats{
        Stats{avg:0., var:0., th_avg:0., th_var:0., number:0}
    }

    pub fn write(&self, mut output:&mut String){
        write!(&mut output, "Average;{:.3};;Th average;{:.3}\n", 
               self.avg, self.th_avg);
        write!(&mut output, "Std deviation;{:.3};;Th std deviation;{:.3}\n", 
               self.var.sqrt(), self.th_var.sqrt());
        write!(&mut output, "Number chunks;{}\n", self.number);
    }
}

impl Clone for Stats{
    fn clone(&self) -> Stats{*self}
}

impl Add for Stats{
    type Output = Stats;

    fn add(self, other:Stats)-> Stats{
        assert!(self.th_avg ==0. || self.th_avg==other.th_avg);
        Stats{
            avg: (self.avg * self.number as f64 + other.avg * other.number as f64) / (self.number + other.number) as f64,
            var :(self.var * self.number as f64 + other.var * other.number as f64) / (self.number + other.number) as f64,

            th_avg : other.th_avg,
            th_var : (self.th_var * self.number as f64 + other.th_var * other.number as f64) / (self.number + other.number) as f64,
            number : self.number + other.number
        }
    }
}

pub struct RabinHasher{
    //We will use a fix window to store data, and it will be the beginning of 
    //this window wich will move.
    //
    //        | the position of the end( begin = 5, in subword of 8bits(u8))     
    // ---------------------
    // |****|****|****|****|  A 4 words window( a word is a u64)
    // ---------------------
    
    end : usize, //it is the position where to add the next sub-word, ie it is the 2^(64*WINDOW_LEN-1) element in the fingerprint
    
    window : [u64;WINDOW_LEN ],
    polynomial : u64, // the irreductible polynomial over GF(2)

    l_fact : u64,

    fingerprint : u64,
    min_size : u64, //min_size of a chunk, if lower it will be aggregated
    max_size : u64, //max_size of a chunk, if greater it will be split with the "max_size" as length, should be at least twice greater than min_size
    
    hasher : Sha256, 
    crc    : u64,

    last_end : u64, //end of the previous stop

    pos : u64,//number of input call
    t_input : f64,
    t_split : f64,
    t_process : f64,
    t_postprocess : f64,
    out_of_bounds : [u64;2],//(number of chuunk splited for maski, number of hnuks agregated for maski)
}

impl RabinHasher{
    pub fn new(min_size:u64, max_size:u64) -> RabinHasher{
        let fact = (((1<<63) % POLYNOMIAL) << 1) % POLYNOMIAL;
        let mut l_fact = fact; 
        
        assert!( ((1 as u64)<<14) * POLYNOMIAL < (1<<63));
        

        for _ in 0..(WINDOW_LEN-1){
            l_fact = (((1 as u64)<<16) * l_fact) % POLYNOMIAL;
            l_fact = (((1 as u64)<<16) * l_fact) % POLYNOMIAL;
            l_fact = (((1 as u64)<<16) * l_fact) % POLYNOMIAL;
            l_fact = (((1 as u64)<<16) * l_fact) % POLYNOMIAL;
        }
        
        let mut hasher = Sha256::new();
                

        assert!(2 * min_size < max_size);

        ///WARNING : Any modificiation here bust propagate to reset
        RabinHasher{
            end : 0,
            
            window : [0; WINDOW_LEN ],
            polynomial : POLYNOMIAL,

            l_fact : l_fact,//pre-coppute 1<<(64 * (WINDOW_LEN-1)) %self.polynomial   
            fingerprint : 0,
            min_size : min_size,
            max_size : max_size,
            out_of_bounds : [0,0],
            
            hasher : hasher,
            crc :0,

            pos : 0,
            last_end : 0,

            t_input : 0.,
            t_split : 0.,
            t_process : 0.,
            t_postprocess : 0.,
        }
    }
    
    fn reset(&mut self){
        self.end = 0;

        for i in 0..WINDOW_LEN{
            self.window[i] = 0; 
        }
        
        self.fingerprint = 0;
       
        self.hasher.reset();
        self.crc = 0;

        self.pos = 0;
        self.last_end = 0;

        self.t_input = 0.;
        self.t_split = 0.;
        self.t_process = 0.;
        self.t_postprocess = 0.;

        self.out_of_bounds[0]=0;
        self.out_of_bounds[1]=0;
    }
    
    fn input(&mut self, x:&u8){
        let start1 = precise_time_s();
        

        //word position 
        //ie it is the position of the working 64bits word in the window 
        let p_w = self.end / (WORD_LEN as usize);     

        //sub-word position
        //ie it is the position of the working 8bits subword in the working word
        let p_sw = (self.end as u32) % WORD_LEN;   

        //mask used to isolate the working subword
        let subword_mask = (0xFF as u64).wrapping_shl(SUBWORD_SIZE*p_sw);
       
        //working-subword
        //if not defined yet(ie for the first called to input), it will be 0 due 
        //to the window initialisation
        let subword:u64 = (self.window[p_w] & subword_mask).wrapping_shr(SUBWORD_SIZE*p_sw);
        
        
        //Update of the fingerprint,
        //No overflow if self.polynomial of degree << 64.
        self.fingerprint = (self.fingerprint + self.polynomial) - ((subword * self.l_fact)%self.polynomial);
        self.fingerprint %= self.polynomial;

        //No overflow if self.polynomial of degree << 56
        self.fingerprint = ((self.fingerprint << 8) + *x as u64) % self.polynomial;


        //Update of the window and of the pointer
        self.window[p_w] = self.window[p_w] & (!subword_mask);
        self.window[p_w] += (*x as u64).wrapping_shl(SUBWORD_SIZE*p_sw);
        self.end = (self.end+1) % (WINDOW_LEN*WORD_LEN as usize);
        
        assert!(self.fingerprint < self.polynomial);
        self.t_input += precise_time_s() - start1;
    }
    
    //end == true : if x is the last byte of the file
    fn split(&mut self, mask:u64, chunks : &mut Vec<Chunk>, x:&u8, end:bool){
        //self.pos is the current position of x into the file
        self.input(x);
        let start1 = precise_time_s();
        
        if (self.fingerprint & mask) == 0 || end{
            let mut chunk = Chunk{
                _type : ChunkType::Data,
                id    : String::new(),
                begin : self.last_end,
                len   : self.pos - self.last_end,
                crc   : 0,
                data  : Vec::new()
            };
            
            if end{
                chunk.len += 1;
                print!("c{} {}\n",chunk.len, self.pos);
                self.hasher.input(&[*x]);
                self.crc = crc64::crc64(self.crc,&[*x]);
                chunks.push(chunk);
                return;
            }

            if chunk.len <= self.min_size{//on privilégie la découpe à l'aggrégation
                self.out_of_bounds[0] += 1;
                self.hasher.input(&[*x]);
                self.crc = crc64::crc64(self.crc,&[*x]);
                return;
            }else{
                self.last_end = self.pos;
                
                chunk.id = self.hasher.result_str();
                chunk.crc = self.crc;

                self.hasher.reset();
                self.crc = 0;
            }
            
            if chunk.len >= self.max_size{//Chunk spliting rule
                let _chunks = chunk.split(self.max_size >> 1);
                self.out_of_bounds[1] += _chunks.len() as u64;  
                chunks.extend(_chunks);
            }else{
                chunks.push(chunk);
            }
           
        }
        self.hasher.input(&[*x]);
        self.crc = crc64::crc64(self.crc,&[*x]);

        //self.pos += 1;
        self.t_split += precise_time_s() - start1;
    }
   
    //conputimng missing hash due to min-max
    //building the hierarchii, and commpute empty hash
    //TODO an external function should be use to set id
    fn postprocess<T:Read+BufRead+Seek>(&mut self, chunks: &mut Vec<Chunk>, reader:&mut T) -> Result<(),Error>{
        let start1 = precise_time_s();
        
        let mut hasher = Sha256::new();
        let mut crc = 0; 
        for chunk in chunks{
            if chunk.id == ""{
                hasher.reset();
                try!(reader.seek(SeekFrom::Start(chunk.begin)));
                let mut len = 0;
                while len < chunk.len{
                    let tmp ={
                        let buff = try!(reader.fill_buf());
                        let tmp = min((chunk.len-len) as usize, buff.len());

                        hasher.input(&buff[0..tmp]);
                        crc = crc64::crc64(crc, &buff[0..tmp]);

                        len+=tmp as u64;
                        
                        if tmp == 0{//TODO
                            panic!("TODO I/O error");
                        }

                        tmp

                    };
                    reader.consume(tmp);
                }
                
                chunk.id = hasher.result_str();
            }
        }
        
        self.t_postprocess = precise_time_s() -start1;
        Ok(())
    }
      
    pub fn process(&mut self, mask:u64, chunks: &mut Vec<Chunk>, reader:&mut BufReader<File>, begin:u64, end:u64)-> Result<(),Error>{
        self.reset();
        
        let start1 = precise_time_s();

        ///IO handling
        //let path = Path::new(location);
        //let file = try!(File::open(&path));
        //let mut reader = BufReader::with_capacity(
        //    min(BUFF_MAX_SIZE as u64, metadata.len()) as usize, file);
        
        let mut hasher = Sha256::new();

        let start2 = precise_time_s();
        let mut j = 0;
        try!(reader.seek(SeekFrom::Start(begin)));
        self.pos=begin;
        self.last_end=begin;
        assert!(end>0);
        'outer: loop{
            let _len = {
                let buff = try!(reader.fill_buf());

                for x in buff{
                    let flag_end = self.pos == end-1;
                    self.split(mask, chunks, x, flag_end);
                    self.pos+=1;
                    if flag_end{
                        break 'outer;
                    }
                }

                buff.len()
            };

            if _len == 0{
                print!("Begin {} end {} {}\n", begin, end, self.pos);
                panic!("Something wrong happen");
                break;
            }
            reader.consume(_len);    
        }
        try!(self.postprocess(chunks, reader));
        let elapsed_time1 = precise_time_s() - start2;
        print!("Elapse buff {}\n ", elapsed_time1);
        self.t_process = precise_time_s() - start1;
        let flag = chunks.last().unwrap().end() == end;
        for chunk in chunks{
            print!("{} {}\n", chunk.begin, chunk.end());
        }
        assert!(flag);

        Ok(())
    }
}
