use bloom::{BloomFilter, ASMS};
use std::{
    collections::HashSet,
    env::args,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    process::exit,
    str::FromStr,
};
use walkdir::WalkDir;

const FILENAME_SAVE: &str = "bigcombo.csv";
const NUM_PARTITIONS: usize = 300; // Dividir inicialmente em 300 filtros menores
const ENTRIES_PER_PARTITION: u32 = i32::MAX as u32; // Máximo de entradas por filtro

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut filepath = PathBuf::new();

    if let Some(valor) = args().nth(1) {
        let path = PathBuf::from_str(&valor)?;
        if !path.exists() {
            eprintln!("Pasta não existe");
            exit(1);
        }
        filepath = path;
    } else {
        eprintln!("Preciso da pasta");
        exit(1);
    }

    let mut file = salvefile();
    let _ = file.write("email,pass\n".as_bytes());

    // Configuração inicial dos filtros de Bloom
    let p = 0.01;
    let mut bloom_filters = Vec::new();
    let mut filter_counts = Vec::new(); // Para contar o número de entradas em cada filtro

    // Criar os filtros de Bloom iniciais
    for _ in 0..NUM_PARTITIONS {
        let filter = BloomFilter::with_rate(p, ENTRIES_PER_PARTITION);
        bloom_filters.push(filter);
        filter_counts.push(0); // Inicializar o contador de entradas para cada filtro
    }
    let mut count_Foda: isize = 0;
    let mut printA: isize = 100000;

    loop {
        for file_entry in WalkDir::new(&filepath) {
            if file_entry.is_err() {
                continue;
            }

            let path = file_entry.unwrap().into_path();

            if path.is_file() {
                println!("{:?}", path);
                if let Ok(hash) = remove_repetidas_open_file(&path) {
                    for string in hash {
                        // Verificar se o valor já existe em algum dos filtros
                        let exists_in_any = bloom_filters.iter().any(|filter| filter.contains(&string));

                        if !exists_in_any {
                            // Tentar inserir nos filtros disponíveis
                            let mut inserted = false;

                            for (index, filter) in bloom_filters.iter_mut().enumerate() {
                                if filter_counts[index] < ENTRIES_PER_PARTITION {
                                    filter.insert(&string);
                                    filter_counts[index] += 1; // Incrementar o contador de entradas
                                    inserted = true;
                                    break;
                                }
                            }

                            // Se não foi inserido, significa que todos os filtros estão saturados, então ignoramos a entrada
                            if !inserted {
                                eprintln!("Todos os filtros estão saturados. Entrada ignorada: {}", string);
                            } else {
                                // Salvar no arquivo
                                let _ = file.write(string.as_bytes());
                                count_Foda +=1;
                                if count_Foda == printA{
                                    println!("Total agora: {}", count_Foda);
                                    printA += printA;
                                }
                            }
                        }
                    }
                }
            }
            let _ = deletefilename(&path);
        }
    }
}

fn remove_repetidas_open_file(pathfile: &PathBuf) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    let mut hashset = HashSet::new();
    let open_file = fs::read_to_string(pathfile)?;
    let proibido = [r#"""#, ",", "'"];

    for line in open_file.lines() {
        let split_line: Vec<&str> = line.split(":").collect();

        if split_line.len() == 2 {
            let mail = split_line[0];
            let pass = split_line[1];

            if proibido.contains(&mail) || proibido.contains(&pass) {
                continue;
            }

            if mail.contains("@") {
                let format_insert_hashmap = format!("\"{}\",\"{}\"\n", mail.to_ascii_lowercase(), pass);
                hashset.insert(format_insert_hashmap);
            }
        }
    }

    Ok(hashset)
}

fn deletefilename(filename: &PathBuf) {
    fs::remove_file(filename).ok();
}

fn salvefile() -> File {
    fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(FILENAME_SAVE)
        .expect("Erro ao abrir arquivo para salvar")
}
