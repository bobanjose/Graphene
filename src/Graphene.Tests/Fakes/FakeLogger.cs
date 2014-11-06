using System;
using Graphene.Configuration;

namespace Graphene.Tests.Fakes
{
        public class FakeLogger : ILogger
        {
            public void Debug(string message)
            {
                System.Diagnostics.Debug.Write(message);
            }

            public void Info(string message)
            {
                System.Diagnostics.Debug.Write(message);
            }

            public void Warn(string message)
            {
                System.Diagnostics.Debug.Write(message);
            }

            public void Error(string message, Exception ex)
            {
                System.Diagnostics.Debug.Write(message);
            }
        } 

}