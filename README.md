

I built the following GUI to demonstrate the issue I'm running into in another project

<img width="796" height="622" alt="Screenshot 2026-01-05 at 2 05 31 PM" src="https://github.com/user-attachments/assets/bed0f7a4-405f-402e-b594-c98339b05d97" />

The goal is for the GUI to show statistics and a progress bar while entries are added to a queue and processed. The issue I am running into is that the 
GUI's progress bar and Entry count do not update while the update to the queue signal is happening. The GUI updates correctly while the queue is being filled, but not while the items are being processed.

In fact, I suspect that the Dioxus executor stalls since the 1s watchdog timer stops printing updates while the Rayon task running in a ```tokio::task::spawn_blocking``` task is executing.

Here is the program output with some comments surrounded by hashmarks that I've added for context on timing

```output
13:46:07 [dev] Full rebuild: triggered manually
13:46:07 [dev] Build completed in 613ms
13:46:08 [macos] Progress use_effect is_progress_active is: false and is_progress_loop_running is: false
13:46:17 [macos] Progress use_effect is_progress_active is: true and is_progress_loop_running is: false
13:46:17 [macos] Starting progress loop
###########################################################################################################
###
### Progress Bar shows up at this point in time in the GUI
### and we can see the number or Pending items increase
###
###########################################################################################################
13:46:17 [macos] Updating progress - obtaining lock
13:46:17 [macos] Updating progress - Received lock in 875ns
13:46:17 [macos] Updating progress - obtaining lock
13:46:17 [macos] Updating progress - Received lock in 1.916µs
13:46:17 [macos] Updating progress - obtaining lock
13:46:17 [macos] Updating progress - Received lock in 2µs
13:46:17 [macos] Updating progress - obtaining lock
13:46:17 [macos] Updating progress - Received lock in 2.209µs
13:46:18 [macos] Updating progress - obtaining lock
13:46:18 [macos] Updating progress - Received lock in 2.208µs
13:46:18 [macos] Updating progress - obtaining lock
13:46:18 [macos] Updating progress - Received lock in 2.416µs
13:46:18 [macos] Updating progress - obtaining lock
13:46:18 [macos] Updating progress - Received lock in 1.792µs
13:46:18 [macos] Updating progress - obtaining lock
13:46:18 [macos] Updating progress - Received lock in 2.875µs
13:46:19 [macos] Updating progress - obtaining lock
13:46:19 [macos] Updating progress - Received lock in 2.083µs
13:46:19 [macos] Updating progress - obtaining lock
13:46:19 [macos] Updating progress - Received lock in 2.166µs
13:46:19 [macos] Updating progress - obtaining lock
13:46:19 [macos] Updating progress - Received lock in 1.958µs
13:46:19 [macos] Loaded Pending Entries in 2.5585075s
###########################################################################################################
###
### Progress Bar is still visible and I would expect the progress to start moving forward here
### We can see 1000 Pending items which correctly reflects what has been queued
###
###########################################################################################################
13:46:19 [macos] Updating progress - obtaining lock
13:46:19 [macos] Updating progress - Received lock in 2.667µs
###########################################################################################################
###
### The progress stalls here while Rayon is iterating over the Pending tasks.. We shoud see the Updating-Progress
### continue to appear every 250ms... something in stalls the GUI while the program is processing the queue
###
###########################################################################################################
13:46:24 [macos] Processed 1000 in 5.344345625s
###########################################################################################################
###
### We can see that after the queue has been processed, the progress monitoring loop resumes 
###
###########################################################################################################
13:46:24 [macos] Updating progress - obtaining lock
13:46:24 [macos] Updating progress - Received lock in 3.125µs
13:46:24 [macos] Updating progress - obtaining lock
13:46:24 [macos] Updating progress - Received lock in 292ns
13:46:24 [macos] Breaking progress loop
13:46:24 [macos] Progress use_effect is_progress_active is: false and is_progress_loop_running is: false

╭──────────────────────────────────────────────────────────────────────────────── /:more ╮
│  App:     ━━━━━━━━━━━━━━━━━━━━━━━━━━  🎉 0.1s      Platform: MacOS                     │
│  Bundle:  ━━━━━━━━━━━━━━━━━━━━━━━━━━  🎉 0.0s      App features: ["desktop"]           │
│  Status:  Serving dioxus-queue-example 🚀 0.1s     Server at: no server address        │
╰────────────────────────────────────────────────────────────────────────────────────────╯
```

Here is the use_effect responsible for updating the progress bar:

```rust
// Update progress display periodically
    {
        let progress_state = progress_state.clone();

        use_effect(move || {
            let is_progress_loop_running = progress_state.lock().unwrap().is_progress_loop_running;
            println!(
                "Progress use_effect is_progress_active is: {} and is_progress_loop_running is: {}",
                is_progress_active(),
                is_progress_loop_running
            );
            if is_progress_active() {
                let progress_state = progress_state.clone();
                let mut progress_display = progress_display.clone();
                let progress_state = progress_state.clone();

                spawn(async move {
                    println!("Starting progress loop");
                    let mut interval = tokio::time::interval(Duration::from_millis(250));
                    {
                        progress_state.lock().unwrap().is_progress_loop_running = true;
                    }
                    loop {
                        interval.tick().await;
                        println!("Updating progress - obtaining lock");
                        let start = Instant::now();
                        let state = progress_state.lock().unwrap().clone();
                        println!("Updating progress - Received lock in {:?}", start.elapsed());
                        let _state_is_complete = state.is_complete();
                        progress_display.set(state);

                        if !is_progress_active() {
                            println!("Breaking progress loop");
                            progress_state.lock().unwrap().is_progress_loop_running = false;
                            break;
                        }
                    }
                });
            }
        });
    }
```

Here is the code that fills and then processes the queue

```rust
let add_pending_entries = move |_| {
        let progress_state = progress_state.clone();
        spawn(async move {
            is_progress_active.set(true);
            let start = Instant::now();

            // Add Pending Entrie to the Queue
            match task::spawn_blocking(move || {
                let new_entries = Vec::<QueueEntry>::new();
                for _ in 1..=1000 {
                    let uuid = Uuid::new_v4();
                    let entry = QueueEntry::Pending(PendingEntry { id: uuid });
                    queue.write().push(entry);
                    thread::sleep(Duration::from_millis(2));
                }
                new_entries
            })
            .await
            {
                Ok(_) => (),
                Err(e) => {
                    println!("Task failed: {}", e);
                }
            };

            println!("Loaded Pending Entries in {:?}", start.elapsed());

            // Process pending files
            // Reset progress
            {
                let mut state = progress_state.lock().unwrap();
                *state = ProgressState::new(queue.len());
            }

            let start = Instant::now();
            let _ = task::spawn_blocking(move || {
                queue.write().par_iter_mut().for_each(|entry| {
                    let is_skipped = rand::rng().random_bool(0.15);
                    match entry {
                        QueueEntry::Pending(_) if is_skipped => {
                            let e = QueueEntry::Skipped(SkippedEntry { id: Uuid::new_v4() });
                            *entry = e;
                        }
                        QueueEntry::Pending(_) if !is_skipped => {
                            let e = QueueEntry::Completed(CompletedEntry { id: Uuid::new_v4() });
                            *entry = e;
                        }

                        _ => (),
                    };
                    // Update progress
                    {
                        let mut state = progress_state.lock().unwrap();
                        state.increment();
                    }
                    thread::sleep(Duration::from_millis(50));
                });

                println!("Processed {} in {:?}", queue.len(), start.elapsed());
            })
            .await;

            is_progress_active.set(false);
        });
    };

```

For more context, I invite you to take a look at the Git Hub repository I've created to support this conversation 
[https://github.com/tgourgon/dioxus-queue-example/tree/main](https://github.com/tgourgon/dioxus-queue-example/tree/main)

I am new to Rust and Dioxus, so I welcome all feedback on style, idiomatic improvements and else.

Thank you in advance for taking the time to help me out!
