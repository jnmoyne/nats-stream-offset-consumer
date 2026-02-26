namespace Synadia.OffsetConsumer;

public class CommitException : Exception
{
    public CommitException(string message) : base(message) { }
    public CommitException(string message, Exception inner) : base(message, inner) { }
}
