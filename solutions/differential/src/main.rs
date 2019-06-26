

fn main() {

    timely::execute_from_args(std::env::args(), |worker| {

        let timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let path = std::env::args().nth(1).expect("Must supply path!");

        let comms = load_data(&format!("{}csv-comments-initial.csv", path), index, peers);
        let knows = load_data(&format!("{}csv-friends-initial.csv", path), index, peers);
        let likes = load_data(&format!("{}csv-likes-initial.csv", path), index, peers);
        let posts = load_data(&format!("{}csv-posts-initial.csv", path), index, peers);
        let users = load_data(&format!("{}csv-users-initial.csv", path), index, peers);

        println!("{:?}\tData loaded! (worker: {})", timer.elapsed(), index);

        // track the progress of our computations
        let mut probe = timely::dataflow::ProbeHandle::new();

        // Build a dataflow for the queries.
        let (mut comms_input, mut knows_input, mut likes_input, mut posts_input, mut users_input) =
        worker.dataflow::<usize,_,_>(|scope| {

            use differential_dataflow::input::Input;
            use differential_dataflow::operators::join::Join;
            use differential_dataflow::operators::reduce::Threshold;
            use differential_dataflow::operators::consolidate::Consolidate;
            use differential_dataflow::operators::reduce::Count;
            use differential_dataflow::operators::reduce::Reduce;

            let (comms_input, comms) = scope.new_collection();
            let (knows_input, knows) = scope.new_collection();
            let (likes_input, likes) = scope.new_collection();
            let (posts_input, posts) = scope.new_collection();
            let (users_input,_users) = scope.new_collection();

            // comms.inspect(|x| println!("Saw: {:?}", x));

            // Query 1: Posts score by comments, and comment likes.
            let liked_comments =
            likes.map(|vec: Vec<String>| (vec[0].clone(), vec[1].clone()))
                 .distinct()
                 .map(|(_user,comm)| (comm, ()))
                 .consolidate()
                 .join(&comms.map(|vec: Vec<String>| (vec[0].clone(), vec[5].clone())))
                 .map(|(_,(_,post))| post)
                 .consolidate()
                //  .inspect(|x| println!("liked: {:?}", x))
                 ;

            let comms_theyselves =
            comms.explode(|vec| Some((vec[5].clone(), 10)));

            let post_score = 
            liked_comments
                .concat(&comms_theyselves)
                .consolidate()
                ;

            use differential_dataflow::hashable::Hashable;

            post_score
                .concat(&posts.map(|vec: Vec<String>| vec[0].clone()))
                .count()
                .map(|(post, count)| (post, count-1))
                .join(&posts.map(|vec: Vec<String>| (vec[0].clone(), vec[1].clone())))
                .map(|(post,(count, ts)): (String, (isize,String))| (post.hashed() % 100, ((count, ts), post)))
                .reduce(|_key, input, output| {
                    for ((_count, post), _wgt) in input.iter().rev().take(3) {
                        output.push(((_count.clone(), post.clone()), 1));
                    }
                })
                .map(|(hash, ((count, ts),post)): (u64, ((isize,String), String))| ((), ((count, ts), post)))
                .reduce(|_key, input, output| {
                    let mut string = format!("{}", (input[input.len()-1].0).1);
                    for ((_count, post), _wgt) in input.iter().rev().skip(1).take(2) {
                        string = format!("{}|{}", string, post);
                    }
                    output.push((string, 1))
                })
                .map(|((), string)| string)
                // .inspect(|x| println!("Q1:\t{:?}", x))
                .probe_with(&mut probe)
                ;


            // Query 2: 
            use differential_dataflow::Collection;
            use differential_dataflow::operators::iterate::Iterate;

            let labels: Collection<_, (String, String, String)> =
            likes           // node         label           comment
                .map(|vec| (vec[0].clone(), vec[0].clone(), vec[1].clone()))
                .iterate(|labels| {

                    let knows = knows.enter(&labels.scope()).map(|vec: Vec<String>| (vec[0].clone(), vec[1].clone()));
                    let likes = likes.enter(&labels.scope()).map(|vec: Vec<String>| (vec[0].clone(), vec[1].clone()));

                    labels
                        .map(|(node, label, comment)| (node, (label, comment)))
                        .join(&knows)
                        .map(|(_node, ((label, comment), dest))| ((dest, comment), label))
                        .semijoin(&likes)
                        .concat(&likes.map(|(user, comm)| ((user.clone(), comm), user)))
                        .reduce(|_key, input, output| {
                            output.push((input[0].0.clone(), 1));
                        })
                        .map(|((dest, comment), label)| (dest, label, comment))

                });

            let comment_score =
            labels
                .map(|(_node, label, comment)| (label, comment))
                .count()
                .explode(|((_label, comment), count)| Some((comment, count * count)))
                .concat(&comms.map(|vec| vec[0].clone()))
                .count()
                .map(|(x, cnt)| (x, cnt-1))
                ;

            comment_score
                .join(&comms.map(|vec| (vec[0].clone(), vec[1].clone())))
                // .map(|(comm, (count, ts))| ((), ((count, ts), comm)))
                .map(|(post,(count, ts)): (String, (isize,String))| (post.hashed() % 100, ((count, ts), post)))
                .reduce(|_key, input, output| {
                    for ((_count, post), _wgt) in input.iter().rev().take(3) {
                        output.push(((_count.clone(), post.clone()), 1));
                    }
                })
                .map(|(hash, ((count, ts),post)): (u64, ((isize,String), String))| ((), ((count, ts), post)))
                .reduce(|_key, input, output| {
                    let mut string = format!("{}", (input[input.len()-1].0).1);
                    for ((_count, post), _wgt) in input.iter().rev().skip(1).take(2) {
                        string = format!("{}|{}", string, post);
                    }
                    output.push((string, 1))
                })
                .map(|((), output)| output)
                // .inspect(|x| println!("Q2:\t{:?}", x))
                .probe_with(&mut probe)
                ;

            (comms_input, knows_input, likes_input, posts_input, users_input)
        });

        for comm in comms {
            comms_input.insert(comm);
        }

        for know in knows {
            knows_input.insert(know);
        }

        for like in likes {
            likes_input.insert(like);
        }

        for post in posts {
            posts_input.insert(post);
        }

        for user in users {
            users_input.insert(user);
        }

        comms_input.advance_to(1); comms_input.flush();
        knows_input.advance_to(1); knows_input.flush();
        likes_input.advance_to(1); likes_input.flush();
        posts_input.advance_to(1); posts_input.flush();
        users_input.advance_to(1); users_input.flush();

        while probe.less_than(comms_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("{:?}\tComputation loaded", timer.elapsed());
        }

        for round in 1 .. 21 {

            // Insert new records!
            let filename = format!("{}change{:02}.csv", path, round);
            // println!("Looking for file: {}", filename);
            let changes = load_data(&filename, index, peers);
            for mut change in changes {
                let collection = change.remove(0);
                match collection.as_str() {
                    "Comments" => { comms_input.insert(change); },
                    "Friends" => { knows_input.insert(change); },
                    "Likes" => { likes_input.insert(change); },
                    "Posts" => { posts_input.insert(change); },
                    "Users" => { users_input.insert(change); },
                    x => { panic!("Weird enum variant: {}", x); },
                }
            }

            comms_input.advance_to(round + 1); comms_input.flush();
            knows_input.advance_to(round + 1); knows_input.flush();
            likes_input.advance_to(round + 1); likes_input.flush();
            posts_input.advance_to(round + 1); posts_input.flush();
            users_input.advance_to(round + 1); users_input.flush();

            while probe.less_than(comms_input.time()) {
                worker.step();
            }

            if index == 0 {
                println!("{:?}\tRound {} complete.", timer.elapsed(), round);
            }
        }

    }).expect("Timely computation failed");
}

fn load_data(filename: &str, index: usize, peers: usize) -> Vec<Vec<String>> {

    // Standard io/fs boilerplate.
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut data = Vec::new();
    let file = BufReader::new(File::open(filename).expect("Could open file"));
    let lines = file.lines();

    for (count, readline) in lines.enumerate() {
        if count % peers == index {
            if let Ok(line) = readline {
                let text : Vec<String> =
                line.split('|')
                    .map(|x| x.to_string())
                    .collect();

                data.push(text);
            }
        }
    }
    // println!("Loaded {} records for {}", data.len(), filename);
    data
}