extern crate mysql;

use std::default::Default;

use mysql::conn::MyOpts;
use mysql::conn::pool::MyPool;
use mysql::value::from_value;

pub mod metal {
    pub trait Metallic {
        fn table(&self) -> &str;
        fn columns(&self) -> Vec<&str>;
    }

    // pub fn all<T>() -> Vec<T> {
    // }
}


#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

impl metal::Metallic for Payment {
    fn table(&self) -> &str {
        "payments"
    }
    fn columns(&self) -> Vec<&str> {
        vec![
            "customer_id",
            "amount",
            "account_name",
        ]
    }
}

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

    // Let's insert payments to the database
    // We will use into_iter() because we does not need to map Stmt to anything else.
    // Also we assume that no error happened in `prepare`.
    for mut stmt in pool.prepare("INSERT INTO tmp.payment (customer_id, amount, account_name) VALUES (?, ?, ?)").into_iter() {
        for p in payments.iter() {
            // Unwrap each result just to make sure no errors happended
            stmt.execute(&[&p.customer_id, &p.amount, &p.account_name]).unwrap();
        }
    }

    // Let's select payments from the database
    let selected_payments: Vec<Payment> = pool.prepare("SELECT customer_id, amount, account_name from tmp.payment")
    .and_then(|mut stmt| { // In this closure we will map `Stmt` to `Vec<Payment>`
        // Here we must use nested combinator because `stmt` must be in scope while working with `QueryResult`
        stmt.execute(&[]).map(|result| { // In this closure we will map `QueryResult` to `Vec<Payment>`
            // QueryResult is iterator over `MyResult<row,err>`
            // so first call to map will map each `MyResult` to contained `row` (no proper error handling)
            // and second call to map will map each `row` to `Payment`
            result.map(|x| x.unwrap()).map(|row| {
                Payment {
                    customer_id: from_value(&row[0]),
                    amount: from_value(&row[1]),
                    account_name: from_value(&row[2]),
                }
            }).collect() // Collect payments so now `QueryResult` is mapped to `Vec<Payment>`
        }) // bubble up `Vec<Payment>` to upper level `and_than`
    }).unwrap(); // Unwrap `Vec<Payment>`

    // Now make shure that `payments` equals `selected_payments`
    // mysql gives no guaranties on order of returned rows without `ORDER BY` 
    // so assume we are lucky
    assert_eq!(payments, selected_payments);
    println!("{:?}", selected_payments);
    println!("Yay!");
}
