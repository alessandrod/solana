use {
    crate::transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
    agave_event_system::{
        event,
        event_metadata::cached_thread_and_process_id,
        publisher::Publisher,
        wincode::{self, SchemaRead, SchemaWrite, config::DefaultConfig},
        wincode_dynamic::{DynPrimitiveTy, PrimitiveTy},
    },
    solana_clock::Slot,
    solana_instruction_error::{CUSTOM_ZERO, InstructionError},
    solana_program_runtime::invoke_context::InstructionExecutionDetails,
    solana_transaction_error::{TransactionError, TransactionResult},
};

/// Replay uses its transaction index as the trace ID and attempt zero.
/// Leader trace IDs identify received copies; attempts identify execution dispatches.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionEventContext {
    pub slot: Slot,
    pub bank_id: u64,
    pub trace_id: u64,
    pub attempt: u32,
}

fn error_variant<T: SchemaWrite<DefaultConfig, Src = T>>(error: &T) -> u32 {
    // The largest SDK error has two u32 tags, a u8 instruction index, and a u32 custom code.
    let mut bytes = [0; 13];
    wincode::serialize_into(bytes.as_mut_slice(), error)
        .expect("SDK errors fit in 13 serialized bytes");
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}

fn error_code(error: &InstructionError) -> u64 {
    match error {
        InstructionError::Custom(0) => CUSTOM_ZERO,
        InstructionError::Custom(code) => u64::from(*code),
        _ => 0,
    }
}

/// Processing outcome retained in the final SVM result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
#[wincode(crate = "agave_event_system::wincode")]
pub enum TransactionProcessingKind {
    Rejected,
    FeesOnly,
    NoOp,
    Executed,
}

impl DynPrimitiveTy for TransactionProcessingKind {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// The phase that reported a transaction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
#[wincode(crate = "agave_event_system::wincode")]
pub enum ExecutionFailureStage {
    BeforeExecution,
    InstructionExecution,
    PostExecution,
}

impl DynPrimitiveTy for ExecutionFailureStage {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// SVM processing results retained through recording and commit.
#[derive(Debug)]
pub struct TransactionProcessingDetails {
    kind: TransactionProcessingKind,
    executed_units: u64,
    charged_units: u64,
    status: TransactionResult<()>,
}

/// One SVM processing outcome per transaction, timestamped when the worker finishes processing.
/// Processing includes recording, commit and synchronous notifications when reached.
/// Excludes replay scheduler handoff and block-cost checks.
/// Batch aborts can discard execution details even when instructions ran.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionProcessingEvent {
    Executed {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        trace_id: u64,
        attempt: u32,
        kind: TransactionProcessingKind,
        /// Instruction execution units retained in the processing result.
        executed_units: u64,
        /// Compute units used for cost accounting, including the configured no-op charge.
        charged_units: u64,
    },
    Failed {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        trace_id: u64,
        attempt: u32,
        kind: TransactionProcessingKind,
        executed_units: u64,
        charged_units: u64,
        stage: ExecutionFailureStage,
        /// Serialized tag of the outer TransactionError variant.
        error_variant: u32,
        /// Custom error in SDK program-return encoding; zero means absent.
        /// Custom(0) is encoded as 1 << 32.
        error_code: u64,
    },
}

/// Entered invocations, including CPIs, builtins and precompiles. IDs are instruction-trace indices.
/// One outcome is published on completion, independently of bank commit. Callees complete before callers.
/// Intervals include nested calls and use the same monotonic clock as other events.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum InstructionExecutionEvent {
    Executed {
        pid: i32,
        tid: i32,
        start_ns: u64,
        end_ns: u64,
        slot: Slot,
        bank_id: u64,
        trace_id: u64,
        attempt: u32,
        instruction_index: u16,
        /// u16::MAX identifies a top-level instruction.
        caller_index: u16,
        program_id: [u8; 32],
        stack_height: u16,
        /// Includes compute units consumed by nested invocations.
        executed_units: u64,
    },
    Failed {
        pid: i32,
        tid: i32,
        start_ns: u64,
        end_ns: u64,
        slot: Slot,
        bank_id: u64,
        trace_id: u64,
        attempt: u32,
        instruction_index: u16,
        caller_index: u16,
        program_id: [u8; 32],
        stack_height: u16,
        executed_units: u64,
        /// Serialized tag of the InstructionError variant.
        error_variant: u32,
        /// Custom error in SDK program-return encoding; zero means absent.
        /// Custom(0) is encoded as 1 << 32.
        error_code: u64,
    },
}

impl From<&TransactionProcessingResult> for TransactionProcessingDetails {
    fn from(result: &TransactionProcessingResult) -> Self {
        let (kind, executed_units, status) = match result {
            Err(error) => (TransactionProcessingKind::Rejected, 0, Err(error.clone())),
            Ok(ProcessedTransaction::FeesOnly(details)) => (
                TransactionProcessingKind::FeesOnly,
                0,
                Err(details.load_error.clone()),
            ),
            Ok(ProcessedTransaction::NoOp(details)) => (
                TransactionProcessingKind::NoOp,
                0,
                Err(details.validation_error.clone()),
            ),
            Ok(ProcessedTransaction::Executed(details)) => (
                TransactionProcessingKind::Executed,
                details.execution_details.executed_units,
                details.execution_details.status.clone(),
            ),
        };

        Self {
            kind,
            executed_units,
            charged_units: result
                .as_ref()
                .map_or(0, ProcessedTransaction::executed_units),
            status,
        }
    }
}

pub fn publish_transaction_processing(
    publisher: &mut Publisher<TransactionProcessingEvent>,
    context: ExecutionEventContext,
    timestamp_ns: u64,
    details: &TransactionProcessingDetails,
) {
    let ExecutionEventContext {
        slot,
        bank_id,
        trace_id,
        attempt,
    } = context;
    let kind = details.kind;
    let executed_units = details.executed_units;
    let charged_units = details.charged_units;
    let (tid, pid) = cached_thread_and_process_id();
    let event = match &details.status {
        Ok(()) => TransactionProcessingEvent::Executed {
            pid,
            tid,
            timestamp_ns,
            slot,
            bank_id,
            trace_id,
            attempt,
            kind,
            executed_units,
            charged_units,
        },
        Err(error) => TransactionProcessingEvent::Failed {
            pid,
            tid,
            timestamp_ns,
            slot,
            bank_id,
            trace_id,
            attempt,
            kind,
            executed_units,
            charged_units,
            stage: match (kind, error) {
                (TransactionProcessingKind::Executed, TransactionError::InstructionError(..)) => {
                    ExecutionFailureStage::InstructionExecution
                }
                (TransactionProcessingKind::Executed, _) => ExecutionFailureStage::PostExecution,
                _ => ExecutionFailureStage::BeforeExecution,
            },
            error_variant: error_variant(error),
            error_code: match error {
                TransactionError::InstructionError(_, error) => error_code(error),
                _ => 0,
            },
        },
    };
    let _ = publisher.publish(&event);
}

pub fn publish_instruction_execution(
    publisher: &mut Publisher<InstructionExecutionEvent>,
    context: ExecutionEventContext,
    instruction: InstructionExecutionDetails,
) {
    let InstructionExecutionDetails {
        start_ns,
        end_ns,
        instruction_index,
        caller_index,
        program_id,
        stack_height,
        executed_units,
        status,
    } = instruction;
    let ExecutionEventContext {
        slot,
        bank_id,
        trace_id,
        attempt,
    } = context;
    let caller_index = caller_index.unwrap_or(u16::MAX);
    let program_id = program_id.to_bytes();
    let (tid, pid) = cached_thread_and_process_id();
    let event = match status {
        Ok(()) => InstructionExecutionEvent::Executed {
            pid,
            tid,
            start_ns,
            end_ns,
            slot,
            bank_id,
            trace_id,
            attempt,
            instruction_index,
            caller_index,
            program_id,
            stack_height,
            executed_units,
        },
        Err(error) => InstructionExecutionEvent::Failed {
            pid,
            tid,
            start_ns,
            end_ns,
            slot,
            bank_id,
            trace_id,
            attempt,
            instruction_index,
            caller_index,
            program_id,
            stack_height,
            executed_units,
            error_variant: error_variant(&error),
            error_code: error_code(&error),
        },
    };
    let _ = publisher.publish(&event);
}
