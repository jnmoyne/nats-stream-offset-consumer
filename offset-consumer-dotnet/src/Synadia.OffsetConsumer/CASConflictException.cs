namespace Synadia.OffsetConsumer;

public class CASConflictException : CommitException
{
    public CASConflictException(Exception inner)
        : base("Offset was modified externally (CAS conflict)", inner) { }
}
