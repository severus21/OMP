use std::io::Error;
use std::fs::{File,metadata};
use std::io::prelude::*;
use std::path::Path;
use std::io::BufReader;
use std::cmp::min;

extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;

extern crate time;
use self::time::precise_time_s;

const BUFF_SIZE:usize = 1024 * 1024 * 64;//16Mo, variable buff size depending on the file size
const wINDOW_LEN:usize = 8;  
const POLYNOMIAL:u64 = 1<<48 + 1<<5 + 1<<3 + 1<<2 + 1;//11;

pub struct RabinHasher{
    begin : usize, //begin of the windows, we will use a cyclic window:w
    
    window : [u64;wINDOW_LEN ],
    polynomial : u64,

    fact : u64,
    l_fact : u64,

    fingerprint : u64,

    b_rotate : f64,
    b_input : f64,
}

const MASK_8_LEFT:u64 =  0xF00000000000000;
const MASK_8_RIGHT:u64 = 0xFFFFFFFFFFFFFF0;
//et si on lisait des mots de 64bits et non de 8 on aurait des rotations plus rapide, suffirait de
//les aggréger avant ???
//

impl RabinHasher{
    pub fn new() -> RabinHasher{
        let fact = (1<<63) % POLYNOMIAL;
        let mut l_fact = fact; 

        for i in 0..(wINDOW_LEN-1){
            l_fact = (fact * l_fact) % POLYNOMIAL;
        }

        RabinHasher{
            begin : 0,
            
            window : [0; wINDOW_LEN ],
            polynomial : POLYNOMIAL,

            fact : fact,//pre-compute 2<<64 % self.polynomial
            l_fact : l_fact,//pre-coppute 2<<(64 * (wINDOW_LEN-1)) %self.polynomial   

            fingerprint : 0,

            b_rotate :0.,
            b_input : 0.,
        }
    }
    
    pub fn rotate_left(&mut self, x: &u8){
        assert!(wINDOW_LEN > 0);
        let start1 = precise_time_s();
        for j in 0..wINDOW_LEN{
            let i = wINDOW_LEN - j -1; //bad, there is a trick with iterator
            //Invariant maintained : window[i+1] already rotated to left, ie 
            //last u8 word of window[i+1] set to 0
            
            let p = self.window[i] & MASK_8_LEFT; //to propagate to left
            if i != wINDOW_LEN-1{
                self.window[i+1] = self.window[i+1] | p; 
            }

            self.window[i] = self.window[i] << 8;//last u8 of ieme is 0
        }
        
        self.window[0] = self.window[0] | (*x as u64);
        self.b_rotate += precise_time_s() - start1;
    }

    pub fn input(&mut self, x:&u8){
        let start1 = precise_time_s();

        let y = self.window[wINDOW_LEN-1] & MASK_8_LEFT ;//null si pas encore complet, du coup pas de pb à l'initialsation
        self.fingerprint = (self.fingerprint + self.polynomial - (y * self.l_fact) % self.polynomial) % self.polynomial;
        self.fingerprint = ((self.fingerprint << 8) % self.polynomial + *x as u64) % self.polynomial;//il faut verifier les bornes mais il probable que le modulo interne ne soit pas nécessaire     

        self.rotate_left(x);

        self.b_input += precise_time_s() - start1;
    }

    pub fn print(&mut self){
        print!("RabinHasher statistics\n");
        print!("Time in rotate : {}s\n", self.b_rotate);
        print!("Time in input  : {}s\n", self.b_input);
    }
}

pub fn rb(location:&str) -> Result<(),Error>{
    let path = Path::new(location);
    let file = try!(File::open(&path));
    let metadata = try!(metadata(location));
    
    let mut reader = BufReader::with_capacity(
        min(BUFF_SIZE as u64, metadata.len()) as usize, file);

    let mut hasher = Sha256::new();
    let mut rb_hasher = RabinHasher::new();
    let mut len = 1;
    let mut pos :u64 = 0; //position into file/stream
    let mut stop_words = Vec::new();
    let start1 = precise_time_s();
    
    while len > 0{
        len = {
            let buff = try!(reader.fill_buf());
            hasher.input(&buff[0..buff.len()]);

            for x in buff{
                rb_hasher.input(x);
                if rb_hasher.fingerprint == 0{//TODO définir le critère de coupe
                    stop_words.push(pos);
                    print!("stopped at {}\n", pos);
                }
                pos += 1;
            }

            buff.len()
        };

        reader.consume(len);    
    }

    let elapsed_time1 = precise_time_s() - start1;
    print!("Elapse buff {}\n ", elapsed_time1);
    print!("Number stop {}/{}\n", stop_words.len(), pos);
    rb_hasher.print();
    Ok(())


}

pub fn rd(location:&str) -> Result<(),Error>{
    let path = Path::new(location);
    let file = try!(File::open(&path));
    let metadata = try!(metadata(location));
    
    let mut reader = BufReader::with_capacity(
        min(BUFF_SIZE as u64, metadata.len()) as usize, file);

    let mut hasher = Sha256::new();
    
    let mut len = 1;
    let start1 = precise_time_s();
    while len > 0{
        len = {
            let buff = try!(reader.fill_buf());
            hasher.input(&buff[0..buff.len()]);
            buff.len()
        };    
        reader.consume(len);    
    }

    let elapsed_time1 = precise_time_s() - start1;
    print!("Elapse buff {}\n ", elapsed_time1);
    print!("{}\n", hasher.result_str());

    Ok(())
}

