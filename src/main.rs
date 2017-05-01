extern crate t;
use t::client::client::Client;
use t::utility::visit_files;

use std::fs::{DirEntry};
use std::path::Path;
fn aux(entry:&DirEntry){
    let mut client = Client::new(String::from("tests/client/data/"));
    let file_id = client.set(entry.path().to_str().unwrap()).unwrap();
    client.get(file_id,"tests/client/data/11-0.txt").unwrap();
}

fn main(){
    let mut client = Client::new(String::from("tests/client/data/"));
    let file_id = client.set("data/txt/11-0.txt").unwrap(); 
    assert_eq!( file_id, "49a0b2726606e1290ac03a63978fa1dd1bd38a8d805704d98265f393533ea094");
    client.remove(file_id.clone()).unwrap();
    client.get(file_id,"tests/client/data/11-0.txt").unwrap();
    //visit_files(Path::new("data/txt/"),&aux);
         
}
