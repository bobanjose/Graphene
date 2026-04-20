using System;
using System.Threading;
using Microsoft.Data.SqlClient;

namespace Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling
{
    public abstract class RetryStrategy
    {
        protected RetryStrategy(int retryCount, TimeSpan initialDelay)
        {
            RetryCount = retryCount;
            InitialDelay = initialDelay;
        }

        public int RetryCount { get; }
        public TimeSpan InitialDelay { get; }

        public virtual TimeSpan GetDelay(int currentRetryCount)
        {
            return InitialDelay;
        }
    }

    public sealed class Incremental : RetryStrategy
    {
        private readonly TimeSpan _increment;

        public Incremental(int retryCount, TimeSpan initialInterval, TimeSpan increment)
            : base(retryCount, initialInterval)
        {
            _increment = increment;
        }

        public override TimeSpan GetDelay(int currentRetryCount)
        {
            return TimeSpan.FromMilliseconds(InitialDelay.TotalMilliseconds + (_increment.TotalMilliseconds * currentRetryCount));
        }
    }

    public sealed class ExponentialBackoff : RetryStrategy
    {
        private readonly TimeSpan _minBackoff;
        private readonly TimeSpan _maxBackoff;
        private readonly TimeSpan _deltaBackoff;

        public ExponentialBackoff(int retryCount, TimeSpan minBackoff, TimeSpan maxBackoff, TimeSpan deltaBackoff)
            : base(retryCount, minBackoff)
        {
            _minBackoff = minBackoff;
            _maxBackoff = maxBackoff;
            _deltaBackoff = deltaBackoff;
        }

        public override TimeSpan GetDelay(int currentRetryCount)
        {
            var multiplier = Math.Pow(2, currentRetryCount) - 1;
            var delayMs = _minBackoff.TotalMilliseconds + (_deltaBackoff.TotalMilliseconds * multiplier);
            if (delayMs > _maxBackoff.TotalMilliseconds)
            {
                delayMs = _maxBackoff.TotalMilliseconds;
            }

            return TimeSpan.FromMilliseconds(delayMs);
        }
    }

    public sealed class RetryingEventArgs : EventArgs
    {
        public required int CurrentRetryCount { get; init; }
        public required TimeSpan Delay { get; init; }
        public required Exception LastException { get; init; }
    }

    public sealed class RetryPolicy<TDetectionStrategy>
    {
        private readonly RetryStrategy _retryStrategy;

        public RetryPolicy(RetryStrategy retryStrategy)
        {
            _retryStrategy = retryStrategy;
        }

        public event EventHandler<RetryingEventArgs>? Retrying;

        internal int RetryCount => _retryStrategy.RetryCount;

        internal TimeSpan GetDelay(int retryCount)
        {
            return _retryStrategy.GetDelay(retryCount);
        }

        internal void OnRetrying(int currentRetryCount, TimeSpan delay, Exception exception)
        {
            Retrying?.Invoke(this, new RetryingEventArgs
            {
                CurrentRetryCount = currentRetryCount,
                Delay = delay,
                LastException = exception
            });
        }
    }

    public sealed class SqlDatabaseTransientErrorDetectionStrategy
    {
    }

    public static class RetrySqlExtensions
    {
        public static void OpenWithRetry(this SqlConnection connection, RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy> retryPolicy)
        {
            ExecuteWithRetry(() => connection.Open(), retryPolicy);
        }

        public static int ExecuteNonQueryWithRetry(this SqlCommand command, RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy> retryPolicy)
        {
            return ExecuteWithRetry(command.ExecuteNonQuery, retryPolicy);
        }

        public static SqlDataReader ExecuteReaderWithRetry(this SqlCommand command, RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy> retryPolicy)
        {
            return ExecuteWithRetry(command.ExecuteReader, retryPolicy);
        }

        private static void ExecuteWithRetry(Action action, RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy> retryPolicy)
        {
            ExecuteWithRetry(() =>
            {
                action();
                return true;
            }, retryPolicy);
        }

        private static T ExecuteWithRetry<T>(Func<T> action, RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy> retryPolicy)
        {
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    return action();
                }
                catch (SqlException ex) when (attempt < retryPolicy.RetryCount)
                {
                    var delay = retryPolicy.GetDelay(attempt + 1);
                    retryPolicy.OnRetrying(attempt + 1, delay, ex);
                    Thread.Sleep(delay);
                }
            }
        }
    }
}
