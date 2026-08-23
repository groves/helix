use std::{
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
    time::Duration,
};

use anyhow::Ok;
use arc_swap::access::Access;

use helix_event::{register_hook, send_blocking};
use helix_view::{
    document::Mode,
    events::DocumentDidChange,
    handlers::{AutoSaveEvent, Handlers},
    Editor,
};
use tokio::time::Instant;

use crate::{
    commands, compositor,
    events::OnModeSwitch,
    job::{self, Jobs},
};

#[derive(Debug)]
pub(super) struct AutoSaveHandler {
    save_pending: Arc<AtomicBool>,
}

impl AutoSaveHandler {
    pub fn new() -> AutoSaveHandler {
        AutoSaveHandler {
            save_pending: Default::default(),
        }
    }
}

impl helix_event::AsyncHook for AutoSaveHandler {
    type Event = AutoSaveEvent;

    fn handle_event(
        &mut self,
        event: Self::Event,
        existing_debounce: Option<tokio::time::Instant>,
    ) -> Option<Instant> {
        match event {
            Self::Event::DocumentChanged { save_after } => {
                Some(Instant::now() + Duration::from_millis(save_after))
            }
            Self::Event::LeftInsertMode => {
                if existing_debounce.is_some() {
                    // If the change happened more recently than the debounce, let the
                    // debounce run down before saving.
                    existing_debounce
                } else {
                    // Otherwise if there is a save pending, save immediately.
                    if self.save_pending.load(atomic::Ordering::Relaxed) {
                        self.finish_debounce();
                    }
                    None
                }
            }
        }
    }

    fn finish_debounce(&mut self) {
        let save_pending = self.save_pending.clone();
        job::dispatch_blocking(move |editor, _| {
            if editor.mode() == Mode::Insert {
                // Avoid saving while in insert mode since this mixes up
                // the modification indicator and prevents future saves.
                save_pending.store(true, atomic::Ordering::Relaxed);
            } else {
                request_auto_save(editor);
                save_pending.store(false, atomic::Ordering::Relaxed);
            }
        })
    }
}

fn request_auto_save(editor: &mut Editor) {
    let mut jobs = Jobs::new();
    let context = &mut compositor::Context {
        editor,
        scroll: Some(0),
        jobs: &mut jobs,
    };

    let options = commands::WriteAllOptions {
        force: false,
        write_scratch: false,
        auto_format: false,
        code_actions: true,
    };

    if let Err(e) = commands::typed::write_all_impl(context, options) {
        context.editor.set_error(format!("{}", e));
    }

    // With code actions on save the write is deferred into a job chain instead of
    // happening synchronously. The `Jobs` above is local to this handler and never
    // polled, so hand those futures to the runtime directly: their callbacks come
    // back through the global job queue and get run by the main loop, which owns the
    // real `Jobs` and picks up the rest of the chain.
    for future in std::mem::take(&mut jobs.wait_futures) {
        tokio::spawn(async move {
            match future.await {
                Result::Ok(Some(callback)) => job::dispatch_callback(callback).await,
                Result::Ok(None) => (),
                Err(err) => helix_event::status::report(err).await,
            }
        });
    }
}

pub(super) fn register_hooks(handlers: &Handlers) {
    let tx = handlers.auto_save.clone();
    register_hook!(move |event: &mut DocumentDidChange<'_>| {
        let config = event.doc.config.load();
        if config.auto_save.after_delay.enable {
            send_blocking(
                &tx,
                AutoSaveEvent::DocumentChanged {
                    save_after: config.auto_save.after_delay.timeout,
                },
            );
        }
        Ok(())
    });

    let tx = handlers.auto_save.clone();
    register_hook!(move |event: &mut OnModeSwitch<'_, '_>| {
        if event.old_mode == Mode::Insert {
            send_blocking(&tx, AutoSaveEvent::LeftInsertMode)
        }
        Ok(())
    });
}
