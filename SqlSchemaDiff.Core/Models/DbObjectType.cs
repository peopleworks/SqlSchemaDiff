namespace SqlSchemaDiff.Models;

public enum DbObjectType
{
    Table,
    View,
    StoredProcedure,
    Function,

    // Added in 1.6. New members are appended so the numeric value of every
    // existing member stays put: a snapshot written by 1.5.0 keeps deserializing
    // whether the consumer serialized the enum by name or by number.
    Trigger,
    Sequence,
    TableType,

    // Added in 1.7, appended for the same reason: a 1.6 snapshot has to keep
    // deserializing, whichever way its producer wrote the enum.
    Synonym
}
