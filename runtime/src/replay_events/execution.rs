use {
    crate::bank::Bank,
    agave_event_system::{
        event, monotonic_timestamp_ns,
        publisher::Publisher,
        stream_name::StreamName,
        wincode::{self, SchemaRead, SchemaWrite, config::DefaultConfig},
        wincode_dynamic::{DynPrimitiveTy, PrimitiveTy},
    },
    solana_clock::Slot,
    solana_instruction::error::{CUSTOM_ZERO, InstructionError},
    solana_program_runtime::invoke_context::InstructionExecutionDetails,
    solana_svm::transaction_processing_result::{
        ProcessedTransaction, TransactionProcessingResult,
    },
    solana_transaction_error::{TransactionError, TransactionResult},
};

pub const TRANSACTION_EXECUTION_STREAM: StreamName =
    agave_event_system::stream_name!("replay.transaction_execution");
pub const INSTRUCTION_EXECUTION_STREAM: StreamName =
    agave_event_system::stream_name!("replay.instruction_execution");

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

/// How far a transaction progressed through processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, SchemaRead, SchemaWrite)]
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
pub enum ExecutionFailureStage {
    BeforeExecution,
    InstructionExecution,
    PostExecution,
    ReplayCheck,
}

impl DynPrimitiveTy for ExecutionFailureStage {
    const TYPE: PrimitiveTy = PrimitiveTy::U32;
}

/// SVM results retained until the scheduler completes replay cost checks.
#[derive(Debug)]
pub struct TransactionExecutionDetails {
    kind: TransactionProcessingKind,
    executed_units: u64,
    charged_units: u64,
    status: TransactionResult<()>,
}

/// One outcome per transaction handled by the scheduler, including replay cost checks.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionExecutionEvent {
    Executed {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
        kind: TransactionProcessingKind,
        /// Instruction execution units, zero when no instructions ran.
        executed_units: u64,
        /// Compute units used for cost accounting, including the configured no-op charge.
        charged_units: u64,
    },
    Failed {
        timestamp_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
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
/// Intervals include nested calls and use the same monotonic clock as other replay events.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum InstructionExecutionEvent {
    Executed {
        start_ns: u64,
        end_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
        instruction_index: u16,
        /// u16::MAX identifies a top-level instruction.
        caller_index: u16,
        program_id: [u8; 32],
        stack_height: u16,
        /// Includes compute units consumed by nested invocations.
        executed_units: u64,
    },
    Failed {
        start_ns: u64,
        end_ns: u64,
        slot: Slot,
        bank_id: u64,
        transaction_index: u64,
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

pub(crate) fn report_processing_results(
    callback: &mut dyn FnMut(usize, TransactionExecutionDetails),
    results: &[TransactionProcessingResult],
) {
    for (batch_index, result) in results.iter().enumerate() {
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
        callback(
            batch_index,
            TransactionExecutionDetails {
                kind,
                executed_units,
                charged_units: result
                    .as_ref()
                    .map_or(0, ProcessedTransaction::executed_units),
                status,
            },
        );
    }
}

pub fn publish_transaction_execution(
    publisher: &mut Publisher<TransactionExecutionEvent>,
    bank: &Bank,
    transaction_index: u64,
    details: &TransactionExecutionDetails,
    replay_error: Option<&TransactionError>,
) {
    let timestamp_ns = monotonic_timestamp_ns();
    let slot = bank.slot();
    let bank_id = bank.bank_id();
    let kind = details.kind;
    let executed_units = details.executed_units;
    let charged_units = details.charged_units;
    let event = match replay_error.or(details.status.as_ref().err()) {
        None => TransactionExecutionEvent::Executed {
            timestamp_ns,
            slot,
            bank_id,
            transaction_index,
            kind,
            executed_units,
            charged_units,
        },
        Some(error) => TransactionExecutionEvent::Failed {
            timestamp_ns,
            slot,
            bank_id,
            transaction_index,
            kind,
            executed_units,
            charged_units,
            stage: if replay_error.is_some() {
                ExecutionFailureStage::ReplayCheck
            } else {
                match (kind, error) {
                    (
                        TransactionProcessingKind::Executed,
                        TransactionError::InstructionError(..),
                    ) => ExecutionFailureStage::InstructionExecution,
                    (TransactionProcessingKind::Executed, _) => {
                        ExecutionFailureStage::PostExecution
                    }
                    _ => ExecutionFailureStage::BeforeExecution,
                }
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
    bank: &Bank,
    transaction_index: u64,
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
    let slot = bank.slot();
    let bank_id = bank.bank_id();
    let caller_index = caller_index.unwrap_or(u16::MAX);
    let program_id = program_id.to_bytes();
    let event = match status {
        Ok(()) => InstructionExecutionEvent::Executed {
            start_ns,
            end_ns,
            slot,
            bank_id,
            transaction_index,
            instruction_index,
            caller_index,
            program_id,
            stack_height,
            executed_units,
        },
        Err(error) => InstructionExecutionEvent::Failed {
            start_ns,
            end_ns,
            slot,
            bank_id,
            transaction_index,
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

#[cfg(test)]
mod tests {
    use {
        crate::replay_events::execution::{error_code, error_variant},
        agave_event_system::wincode,
        solana_instruction::error::{CUSTOM_ZERO, InstructionError},
        solana_transaction_error::TransactionError,
    };

    #[test]
    fn test_error_variants_and_codes() {
        for index in 0..=error_variant(&InstructionError::BuiltinProgramsMustConsumeComputeUnits) {
            let mut bytes = [0; 13];
            bytes[..4].copy_from_slice(&index.to_le_bytes());
            let error = wincode::deserialize_from::<InstructionError>(bytes.as_slice()).unwrap();
            assert_eq!(error_variant(&error), index);
            if !matches!(error, InstructionError::Custom(_)) {
                assert_eq!(error_code(&error), 0);
            }
        }
        for index in 0..=error_variant(&TransactionError::CommitCancelled) {
            let mut bytes = [0; 13];
            bytes[..4].copy_from_slice(&index.to_le_bytes());
            let error = wincode::deserialize_from::<TransactionError>(bytes.as_slice()).unwrap();
            assert_eq!(error_variant(&error), index);
        }
        assert_eq!(
            error_variant(&InstructionError::Custom(u32::MAX)),
            error_variant(&InstructionError::Custom(0)),
        );
        assert_eq!(
            error_variant(&TransactionError::InstructionError(
                u8::MAX,
                InstructionError::Custom(u32::MAX),
            )),
            error_variant(&TransactionError::InstructionError(
                0,
                InstructionError::Custom(0),
            )),
        );
        assert_eq!(error_code(&InstructionError::Custom(0)), CUSTOM_ZERO);
        assert_eq!(error_code(&InstructionError::Custom(1)), 1);
        assert_eq!(
            error_code(&InstructionError::Custom(u32::MAX)),
            u64::from(u32::MAX),
        );
    }
}
