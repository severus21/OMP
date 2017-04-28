extern crate t;
use t::client::client::Client;


fn main(){
    let mut client = Client::new(String::from("tests/client/data/"));
    let file_id = client.set("data/txt/11-0.txt").unwrap(); 
    assert_eq!( file_id, "49a0b2726606e1290ac03a63978fa1dd1bd38a8d805704d98265f393533ea094");
    client.get(file_id,"tests/client/data/11-0.txt").unwrap();
         
}
