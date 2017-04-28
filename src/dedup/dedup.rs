//use dedup::rabin::{RabinHasher, Stats};
//use dedup::chunk::{HChunk, Chunk, HEADER_LEN_ENTRY};

use std::io::prelude::*;
use std::io;
use std::fs::{self, File};
use std::path::Path;
use std::fmt::{Write as FmtWrite};
use std::collections::{HashMap};

use std::cmp::max;
extern crate mime_guess;
use self::mime_guess::{guess_mime_type, get_mime_type, Mime};

extern crate mime;
use self::mime::{TopLevel, SubLevel};

pub struct Rule{
    pub masks       : Vec<u64>,
    //Masks should be ordered in decreasing order
    pub min_size    : u64,
    pub max_size    : u64,
}
impl Clone for Rule{
    fn clone(&self)-> Rule{
        Rule{
            masks : Vec::from(&self.masks[..]),
            min_size : self.min_size,
            max_size : self.max_size
        }
    }
} 

pub struct Phi{
    rules : HashMap<TopLevel, HashMap<SubLevel, Rule>>
    //mime => rule(masks, min_sizen max_size)}
}

impl Phi{
    pub fn new() -> Phi{
        let mut rules = HashMap::new();
        
        {
            let mut text_rules = HashMap::new();
            
            text_rules.insert(SubLevel::Plain, Rule{
                masks : vec![0b111111111111111, //avg : 32Ko
                             //0b111111111111
                ], //avg 4Ko 
                min_size : 1<<10,
                max_size : 1<<17}); 
                                        //1<<10, //min 1Ko
                                        //1<<17));  //max 128Ko );

            rules.insert(TopLevel::Text, text_rules);
        }

        {
            let mut app_rules = HashMap::new();
            app_rules.insert(SubLevel::OctetStream, Rule{
                masks : vec![0b11111111111111111, //avg : 128Ko
                             0b111111111111111, //avg : 32Ko
                             0b111111111111], //avg 4Ko 
                min_size : 1<<10, //min 1Ko,
                max_size : 1<<20}); //max 1Mo

            rules.insert(TopLevel::Application, app_rules);
        }

        Phi{
            rules : rules
        }
    }

    pub fn hasher(&self, filename : &str ) -> Rule{
        let mut mime = guess_mime_type(filename);
        let rule =  if !self.rules.contains_key(&mime.0){
            &self.rules[&TopLevel::Application][&SubLevel::OctetStream]
        }else{
            if !self.rules[&mime.0].contains_key(&mime.1){
                &self.rules[&TopLevel::Application][&SubLevel::OctetStream]
            }else{
                &self.rules[&mime.0][&mime.1]
            }
        };
        rule.clone() 
    }
}







/*



struct FileStats{
    location : String,
    mime : Mime,
    size : u64,
}

struct ChunkStats{
    chunk : Chunk,
    mask_lvl : usize,
    pub fstats : Vec<FileStats>,    
}

impl ChunkStats{
    //Return the size( in Bytes) of this chunk in the whole system
    //Ie in the Data and in the Index of the file
    //N.B :  Not the size of indirection in the hierarchy
    pub fn system_size(&self) -> u64{
        //self.chunk.size + HEADER_LEN_ENTRY * self.fstats.len() as u64
        0
            //TODO
    }
}

pub struct PhiEstimator{
    hasher : RabinHasher,
    full_sizes : HashMap<Mime, u64>, //size on disk of the db
    hasher_stats : (usize, usize, Vec<Stats>),
    db: HashMap<String, ChunkStats>,
}

//TODO stats : type de chunk en fonction du poids
impl PhiEstimator{
    pub fn new(masks:Vec<u64>, min_size:u64, max_size:u64) -> PhiEstimator{
        PhiEstimator{
            full_sizes : HashMap::new(),
            hasher_stats : (0, 0, vec![Stats::new(); masks.len()]),
            db : HashMap::new(),
            hasher : RabinHasher::new(masks, min_size, max_size),
        }
    }

    fn add(&mut self, hchunk: &HChunk, mime_type : &Mime, location : String, 
        file_size :u64){

        if !self.db.contains_key(&hchunk.id){
            let chunk = Chunk{
                _type   : hchunk._type(),
                id      : hchunk.id.clone(), 
                begin   : hchunk.begin, 
                len     : hchunk.len, 
                data    : Vec::new(),
            };

            for subchunk in &hchunk.subchunks{
                self.add( subchunk, mime_type, location.clone(), file_size);
            }

            self.db.insert(hchunk.id.clone(), ChunkStats{
                chunk: chunk ,
                mask_lvl : hchunk.lvl,    
                fstats : vec![FileStats{
                    location: location,
                    mime: mime_type.clone(),
                    size : file_size
                }]    
            });
        }else{
            let chunkStats = self.db.get_mut(&hchunk.id).unwrap(); //EXISTS
            chunkStats.fstats.push(FileStats{
                location: location,
                mime: mime_type.clone(),
                size : file_size
            });
        }
    }

    fn scandir(&mut self, dir: &Path) -> io::Result<()> {
        if dir.is_dir() {
            for entry in try!(fs::read_dir(dir)) {
                let entry = try!(entry);
                let path = entry.path();
                if path.is_dir() {
                    try!(self.scandir(&path));
                } else {
                    let location = match entry.path().to_str() {
                        None => continue,
                        Some(_str) => String::from(_str)
                    };
                    
                    let mut hierachy = Vec::new();
                    let _ = self.hasher.process(&mut hierachy, &location); 
                    let metadata = try!(entry.metadata());
                    let mut mime_type = guess_mime_type(&location);
                    if !self.full_sizes.contains_key(&mime_type){
                        self.full_sizes.insert(mime_type.clone(), metadata.len());
                    }else{
                        *self.full_sizes.get_mut(&mime_type).unwrap() += metadata.len();
                    }
                    for hchunk in hierachy{
                        self.add(&hchunk, &mime_type, location.clone(),
                                metadata.len());
                    }
                    /*let (ref mut s_ram, ref mut c_ram, ref mut stats) = self.hasher_stats;
                    let (n_s_ram, n_c_ram, n_stats) =self.hasher.stats();
                    *s_ram = n_s_ram;//TODO avg??
                    *c_ram = max(*c_ram, n_c_ram);
                    for i in 0..stats.len(){
                        stats[i] = stats[i] + n_stats[i];
                    }*/
                }
            }
        }
        Ok(())
    }
    
    pub fn stats_hasher(&self) -> io::Result<()>{
        let mut buffer = try!(File::create("hasher-stats.csv"));
        let mut output = String::new();
        
        write!(&mut output, "Hasher stats\n");
        write!(&mut output, "Static ram;{}\nMax ram;{}\n", self.hasher_stats.0, 
               self.hasher_stats.1);
        for stat in &self.hasher_stats.2{
            stat.write(&mut output);
        }

        try!(buffer.write_all(output.as_bytes()));
        Ok(()) 
    }
    
    pub fn stats_dedup(&self) -> io::Result<()>{
        let mut buffer = try!(File::create("phi-stats.csv"));
        let mut output = String::new();
        
        write!(&mut output, "Deduplication Stats\n\n");

        let mut g_files = 0;
        let mut g_full_size = 0;
        let mut g_system_size = 0;
        
        //mime_type => (saved_space agregated over all chunks in db, classified
        //according to mask lvl)
        let mut mime_stats = HashMap::new();
        
        for chunkStats in self.db.values(){
            let system_size = chunkStats.system_size();
            
            

            for fileStats in &chunkStats.fstats{
                if !mime_stats.contains_key(&fileStats.mime){
                    let mut mask_stats = vec![0;self.hasher.spliting_rules.len()];
                    mask_stats[chunkStats.mask_lvl] = system_size;

                    mime_stats.insert(fileStats.mime.clone(), (
                                                               system_size, 
                                                               mask_stats));
                }else{
                    let ref mut stats = mime_stats.get_mut(&fileStats.mime).unwrap();//EXISTS
                    
                    stats.0 += system_size;
                    assert!(chunkStats.mask_lvl < stats.1.len());
                    *stats.1.get_mut(chunkStats.mask_lvl).unwrap() += system_size;
                    //OUGHT TO EXISTS
                }
            }
        }
        for (mime, (system_size, mask_stats)) in mime_stats{
            g_system_size += system_size;
            g_full_size += self.full_sizes[&mime];
            write!(&mut output, "MIME-type {};\n", mime);
            write!(&mut output, "Files;TODO\n");
            write!(&mut output, "Full size ;{};\n", self.full_sizes[&mime]);
            write!(&mut output, "System size ;{};\n", system_size);
            write!(&mut output, "Saved;{:.4};\n", 
                   1. - system_size as f64 / self.full_sizes[&mime] as f64);

            for (i, system_size) in mask_stats.into_iter().enumerate(){
                write!(&mut output, "Mask;;Saved Size;Saved\n");
                write!(&mut output, "{};;{}{:.4};\n",
                    self.hasher.spliting_rules[i],
                    system_size,  
                    1. - system_size as f64 / self.full_sizes[&mime] as f64);

            }
        }
                
        write!(&mut output, "\nGlobal deduplication stats\n");
        
        write!(&mut output, "Files;TODO\n");
        write!(&mut output, "Full size ;{};\n", g_full_size);
        write!(&mut output, "System size ;{};\n", g_system_size);
        write!(&mut output, "Saved;{:.4};\n", 
               1. - g_system_size as f64 / g_full_size as f64); 

        try!(buffer.write_all(output.as_bytes()));
        Ok(())
    }
    
    pub fn stats(&self) -> io::Result<()>{
        self.stats_dedup();
        self.stats_hasher()
    }
    pub fn estimate(&mut self, location:&str) {
        let _=self.scandir(Path::new(location)); 
        self.stats();
    }

    
}*/
