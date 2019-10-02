//using Nest;
using System.Collections.Generic;

namespace EventLogApp
{
    internal class EventElementsDictionary<T> : Dictionary<long, T> where T : CodeNameType
    {
        //private Dictionary<int, T> dictionary = new Dictionary<int, T>();

        public EventElementsDictionary() : base() { }

        EventElementsDictionary(int capacity) : base(capacity) { }

        public void Add(T item)
        {
            if (typeof(T).IsSubclassOf(typeof(CodeNameGuidType)))
            {
                if (base.ContainsKey(item.Code))
                {
                    base.Remove(item.Code);
                }
            }

            base.Add(item.Code, item);
        }
    }
}
