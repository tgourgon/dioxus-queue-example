use dioxus::prelude::*;
use rand::Rng;
use rayon::prelude::*;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tokio::task;
use uuid::Uuid;

#[derive(Debug, Clone, Routable, PartialEq)]
#[rustfmt::skip]
enum Route {
    #[route("/")]
    Home {}
}

const FAVICON: Asset = asset!("/assets/favicon.ico");
const MAIN_CSS: Asset = asset!("/assets/main.css");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

#[derive(Clone, PartialEq)]
struct PendingEntry {
    id: Uuid,
}

#[derive(Clone, PartialEq)]
struct SkippedEntry {
    id: Uuid,
}

#[derive(Clone, PartialEq)]
struct CompletedEntry {
    id: Uuid,
}

#[derive(Clone, PartialEq)]
enum QueueEntry {
    Pending(PendingEntry),
    Skipped(SkippedEntry),
    Completed(CompletedEntry),
}

// Progress tracking struct
#[derive(Clone, Default)]
struct ProgressState {
    is_progress_loop_running: bool,
    current: usize,
    total: usize,
}

impl ProgressState {
    fn new(total: usize) -> Self {
        Self {
            is_progress_loop_running: false,
            current: 0,
            total,
        }
    }

    fn increment(&mut self) {
        self.current = self.current.saturating_add(1);
    }

    fn fraction(&self) -> f32 {
        if self.total == 0 {
            0.0
        } else {
            self.current as f32 / self.total as f32
        }
    }

    fn is_complete(&self) -> bool {
        self.current >= self.total
    }
}

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! {
        document::Link { rel: "icon", href: FAVICON }
        document::Link { rel: "stylesheet", href: MAIN_CSS }
        document::Link { rel: "stylesheet", href: TAILWIND_CSS }
        Router::<Route> {}
    }
}

#[component]
fn Home() -> Element {
    let mut queue = use_signal_sync(Vec::<QueueEntry>::new);

    // Progress tracking
    let mut is_progress_active = use_signal(|| false);
    let progress_state = use_context_provider(|| Arc::new(Mutex::new(ProgressState::default())));
    let progress_display = use_signal(ProgressState::default);

    //Watchdog Run/Stop signal
    let mut watchdog = use_signal(|| false);

    // File type counts
    let entry_counters = use_memo(move || {
        let mut pending = 0;
        let mut completed = 0;
        let mut skipped = 0;

        for file in queue.read().iter() {
            match file {
                QueueEntry::Pending(_) => pending += 1,
                QueueEntry::Completed(_) => completed += 1,
                QueueEntry::Skipped(_) => skipped += 1,
            }
        }

        (pending, completed, skipped)
    });

    use_effect(move || {
        if watchdog() {
            spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(1000));
                let timer = Instant::now();
                loop {
                    interval.tick().await;
                    println!(
                        "Watchdog Timer Tick, elapsed time is {:?}... GUI is still responsive",
                        timer.elapsed()
                    );
                    if !watchdog() {
                        break;
                    }
                }
            });
        }
    });

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
            if is_progress_active() && !is_progress_loop_running {
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
                            {
                                let mut state = progress_state.try_lock().unwrap();
                                state.is_progress_loop_running = false;
                                state.current = 0;
                                state.total = 0;
                            }
                            break;
                        }
                    }
                });
            }
        });
    }

    let add_pending_entries = move |_| {
        let progress_state_final_reset = progress_state.clone();
        let progress_state = progress_state.clone();
        spawn(async move {
            is_progress_active.set(true);
            let start = Instant::now();

            // Add Pending Entries to the Queue
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

            // Process pending entries
            // Reset progress
            {
                let mut state = progress_state.lock().unwrap();
                *state = ProgressState::new(queue.len());
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel(100);
            let start = Instant::now();
            let handle = task::spawn_blocking(move || {
                let entries: Vec<(usize, QueueEntry)> =
                    queue.read().iter().cloned().enumerate().collect();

                entries.par_iter().for_each(|entry_tuple| {
                    let tx = tx.clone();
                    let is_skipped = rand::rng().random_bool(0.15);
                    let res = match entry_tuple {
                        (idx, QueueEntry::Pending(_)) if is_skipped => (
                            *idx,
                            QueueEntry::Skipped(SkippedEntry { id: Uuid::new_v4() }),
                        ),
                        (idx, QueueEntry::Pending(_)) if !is_skipped => (
                            *idx,
                            QueueEntry::Completed(CompletedEntry { id: Uuid::new_v4() }),
                        ),
                        (idx, ent) => (*idx, ent.clone()),
                    };
                    let _ = tx.blocking_send(res);
                    thread::sleep(Duration::from_millis(20));
                });
            });

            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let (idx, entry) = msg;
                    queue.write()[idx] = entry;

                    // Update progress
                    {
                        let mut state = progress_state.lock().unwrap();
                        state.increment();
                    }
                }
            });

            handle.await.unwrap();
            println!("Processed {} in {:?}", queue.len(), start.elapsed());
            // Reset progress
            {
                let mut state = progress_state_final_reset.lock().unwrap();
                *state = ProgressState::new(queue.len());
            }
            is_progress_active.set(false);
        });
    };

    let (pending, completed, skipped) = *entry_counters.read();
    let progress = progress_display.read();

    rsx! {
        div { class: "p-4 pt-10 space-y-4",
        div {
            class: "flex gap-4",
            // Add pending entries button
            label {
                class: "inline-block px-4 py-2 bg-blue-500 text-white rounded cursor-pointer hover:bg-blue-600 transition-colors",
                for: "file-upload",
                "Load Pending Entries"
            }
            button {
                class: "hidden",
                id: "file-upload",
                onclick: add_pending_entries,
            }
            // Start/Stop Watchdog
            label {
                class: "inline-block px-4 py-2 bg-blue-500 text-white rounded cursor-pointer hover:bg-blue-600 transition-colors",
                for: "watchdog_btm",
                "Start/Stop Watchdog Timer"
            }
            button {
                class: "hidden",
                id: "watchdog_btm",
                onclick: move |_| watchdog.toggle(),
            }


        }
            // Progress indicators
            if is_progress_active() {
                div { class: "space-y-2",
                    div { class: "text-sm font-medium", "Processing entries..." }
                    div { class: "w-full bg-gray-200 rounded-full h-2",
                        div {
                            class: "bg-blue-600 h-2 rounded-full transition-all duration-300",
                            style: "width: {progress.fraction() * 100.0}%",
                        }
                    }
                    div { class: "text-sm text-gray-600",
                        "Processed {progress.current} of {progress.total}"
                    }
                }
            },
            // Statistics
            div { class: "grid grid-cols-4 gap-4 text-sm",
                div { class: "p-3 bg-gray-50 rounded",
                    div { class: "font-medium", "Total" }
                    div { class: "text-xl", "{queue.read().len()}" }
                }
                div { class: "p-3 bg-yellow-50 rounded",
                    div { class: "font-medium", "Pending" }
                    div { class: "text-xl", "{pending}" }
                }
                div { class: "p-3 bg-green-50 rounded",
                    div { class: "font-medium", "Completed" }
                    div { class: "text-xl", "{completed}" }
                }
                div { class: "p-3 bg-red-50 rounded",
                    div { class: "font-medium", "Skipped" }
                    div { class: "text-xl", "{skipped}" }
                }
            }
        }
    }
}
