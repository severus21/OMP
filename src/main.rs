extern crate t;

use t::dedup::rabin;

fn main(){
    //rabin::rb("/home/severus/Downloads/O.mkv");
    rabin::rb("txt/about.txt");
    print!("Coucou")
}
