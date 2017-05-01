use std::io;
use std::fs::{self, DirEntry};
use std::path::Path;

pub fn visit_files(dir:&Path,  f:&Fn(&DirEntry)) -> io::Result<()>{
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
