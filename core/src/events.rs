use agave_event_system::{
    Event, EventSystem, PublisherFactory, StreamConfig, stream_name::StreamName,
};

pub(crate) fn create_event_stream<E: Event>(
    event_system: &EventSystem,
    stream_name: StreamName,
    config: StreamConfig,
) -> PublisherFactory<E> {
    event_system
        .create_stream(stream_name.clone(), config)
        .unwrap_or_else(|err| {
            error!(
                "Failed to create event stream {}: {err:?}; disabling this stream",
                stream_name.as_str()
            );
            EventSystem::stub()
                .create_stream(stream_name, config)
                .expect("stub event stream creation is infallible")
        })
}
