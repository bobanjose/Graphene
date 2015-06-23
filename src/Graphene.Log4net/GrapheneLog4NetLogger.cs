using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using Gr = Graphene.Configuration;

namespace Graphene.Configuration
{
    public class GrapheneLog4NetLogger : Gr.ILogger
    {
        private ILog _logger;

        public GrapheneLog4NetLogger()
        {
            _logger = log4net.LogManager.GetLogger("Graphene");
        }


        public void Debug(string message)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug(message);
        }

        public void Info(string message)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info(message);
        }

        public void Warn(string message)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn(message);
        }

        public void Error(string message, Exception ex)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error(message + "---" + ex.StackTrace);
        }
    }
}