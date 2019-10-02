namespace EventLogApp
{
    public static class StringExtension
    {
        public static string RemoveQuotes(this string str)
        {
            string retval = str;

            if (retval.StartsWith("\""))
            {
                retval = retval.Substring(1);
            }

            if (retval.EndsWith("\""))
            {
                retval = retval.Substring(0, retval.Length - 1);
            }

            return retval;
        }
    }
}
