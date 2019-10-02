namespace EventLogApp
{
    internal class CodeNameGuidType : CodeNameType
    {
        public string Guid;

        public CodeNameGuidType(long code, string name, string guid) : base(code, name)
        {
            this.Guid = guid;
        }
    }
}
