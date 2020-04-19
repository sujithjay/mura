// Copyright 2020 Sujith Jay Nair
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


use rustyline::error::ReadlineError;
use rustyline::Editor;

mod parser;
mod planner;

fn main() {
    let mut rl = Editor::<()>::new();
    if rl.load_history("mura.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                if line.len() == 0 {
                    print!("");
                 } 
                else {
                    rl.add_history_entry(line.as_str());
                    let statement = parser::parse(line).ok();
                    println!("{:?}", statement.unwrap());
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("Bye!");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("See you!");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    rl.save_history("mura.txt").unwrap();
}
