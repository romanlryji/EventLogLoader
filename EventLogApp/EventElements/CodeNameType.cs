namespace EventLogApp
{
    internal class CodeNameType
    {
        public long Code;
        public string Name;

        public CodeNameType(long code, string name)
        {
            this.Code = code;

            if (string.IsNullOrEmpty(name))
            {
                this.Name = string.Empty;
            }
            else
            {
                this.Name = name;
            }
        }
    }
}
