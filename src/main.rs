extern crate mysql;

use std::default::Default;

use mysql::conn::MyOpts;
use mysql::conn::pool::MyPool;
use mysql::value::from_value;

#[macro_use]
pub mod metal {

    macro_rules! oxide {
        (
            $name:ident,
            $table:expr,
            // Note the extra trailing comma to allow last field to have comma
            [ $( $idx:expr, $ex:ident: $ty:ty ),* ,]
        ) => {
            // Define the struct with fields
            #[derive(Debug, PartialEq, Eq)]
            struct $name {
                $( $ex: $ty ),*
            }

            impl $name {
                fn table() -> &'static str {
                    $table
                }

                // Build an instance of $name from row data
                fn from_row(row: Vec<mysql::value::Value>) -> $name {
                    $name {
                        $( $ex: from_value(&row[$idx]) ),*
                    }
                }

                // Return a Vec<str> of all of the columns for the struct
                fn columns() -> Vec<&'static str> {
                    vec![
                        $( stringify!($ex) ),*
                    ]
                }

                fn concatenated_columns() -> &'static str {
                    stringify!($( $ex ), *)
                }

                fn value_placeholders() -> String {
                    $name::columns().iter().fold(
                        "(".to_string(), |acc, _| {
                            match acc.len() {
                                1 => { acc + "?" },
                                _ => { acc + ", ?" }
                            }
                        }
                    ) + ")"
                }

                pub fn insert_all(pool: &MyPool, items: &Vec<$name>) {
                    // Let's insert payments to the database
                    // We will use into_iter() because we does not need to map Stmt to anything
                    // else.
                    // Also we assume that no error happened in `prepare`.
                    let query = "INSERT INTO ".to_string() +
                        $name::table() +
                        " (" +
                        $name::concatenated_columns() +
                        ") VALUES " +
                        &$name::value_placeholders();
                    println!("{}", query);
                    for mut stmt in pool.prepare(query).into_iter() {
                        for i in items.iter() {
                            // Unwrap each result just to make sure no errors happended
                            stmt.execute(&[
                                $( &i.$ex ),*,
                            ]).unwrap();
                        }
                    }
                }

                pub fn all(pool: MyPool) -> Vec<$name> {
                    let query = "SELECT ".to_string() +
                        &$name::concatenated_columns() +
                        " FROM " +
                        &$name::table();
                    println!("{}", query);
                    let results: Vec<$name> = pool.prepare(query).
                        and_then(| mut stmt| {
                            stmt.execute(&[]).map(|result| {
                                result.map(|x| x.unwrap()).map(|row| {
                                    $name::from_row(row)
                                }).collect()
                            })
                        }).unwrap();

                    return results;
                }
            }
        }
    }
}

oxide!(
    Payment,
    "tmp.payment",
    [
        0, customer_id: i32,
        1, amount: i32,
        2, account_name: Option<String>,
    ]
);


fn main() {
    let opts = MyOpts {
        user: Some("root".into()),
        ..Default::default()
    };
    let pool = MyPool::new(opts).unwrap();

    for mut stmt in pool.prepare("CREATE TEMPORARY TABLE tmp.payment (customer_id int not null, amount int not null, account_name text)").into_iter() {
        // Unwap just to make sure no error happened
        stmt.execute(&[]).unwrap();
    }

    let payments = vec![
        Payment { customer_id: 1, amount: 2, account_name: None },
        Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
        Payment { customer_id: 5, amount: 6, account_name: None },
        Payment { customer_id: 7, amount: 8, account_name: None },
        Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
    ];

    Payment::insert_all(&pool, &payments);

    let selected_payments = Payment::all(pool);

    // Now make shure that `payments` equals `selected_payments`
    // mysql gives no guaranties on order of returned rows without `ORDER BY` 
    // so assume we are lucky
    assert_eq!(payments, selected_payments);
    println!("{:?}", selected_payments);
    println!("Yay!");
}
