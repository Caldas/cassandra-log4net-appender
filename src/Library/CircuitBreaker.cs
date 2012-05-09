using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraLog4NetAppenderLibrary
{
    public class CircuitBreaker
    {
        private CircuitState state = CircuitState.CLOSED;
        private Int64 nextCheck = 0L;
        private Int32 failureRate = 0;
        private Int32 threshold = 1;
        private Int32 timeoutInSecs = 1;

        public CircuitBreaker(Int32 threshold, Int32 timeoutInSecs)
        {
            if (threshold > 0)
                this.threshold = threshold;

            if (timeoutInSecs > 0)
                this.timeoutInSecs = timeoutInSecs;
        }

        public Boolean Allow()
        {
            if ((this.state == CircuitState.OPEN) && (this.nextCheck < DateTime.Now.Ticks / 1000L))
                this.state = CircuitState.HALF_OPEN;

            return (this.state == CircuitState.CLOSED) || (this.state == CircuitState.HALF_OPEN);
        }

        public void Success()
        {
            if (this.state == CircuitState.HALF_OPEN)
                Reset();
        }

        public void Failure()
        {
            if (this.state == CircuitState.HALF_OPEN)
                Trip();
            else
            {
                this.failureRate += 1;

                if (this.failureRate >= this.threshold)
                    Trip();
            }
        }

        private void Reset()
        {
            this.state = CircuitState.CLOSED;
            this.failureRate = 0;
        }

        private void Trip()
        {
            if (this.state != CircuitState.OPEN)
            {
                this.state = CircuitState.OPEN;
                this.nextCheck = (DateTime.Now.Ticks / 1000L + this.timeoutInSecs);
            }
        }
    }

    public enum CircuitState
    {
        CLOSED,
        HALF_OPEN,
        OPEN
    }
}