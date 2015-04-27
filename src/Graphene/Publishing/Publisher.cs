﻿// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Graphene.Data;
using Graphene.Tracking;

namespace Graphene.Publishing
{
    internal class Publisher
    {
        private static readonly ActionBlock<ContainerBase> _trackerBlock;
        private static readonly ActionBlock<TrackerData> _publisherBlock;

        private static ContainerBase _firstTC;

        private static readonly CancellationTokenSource _trackerBlockCancellationTokenSource =
            new CancellationTokenSource();

        private static bool _lastPersistanceComplete;
        private static bool _trackersRegisted;
        private static DateTime lastPersistTime = DateTime.UtcNow;
        
        static Publisher()
        {
            _trackerBlock = new ActionBlock<ContainerBase>((Func<ContainerBase, Task>) MeasureAccumulator);

            _publisherBlock = new ActionBlock<TrackerData>((Action<TrackerData>) trackerWriter, new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = 4});
        }

        private static void trackerWriter(TrackerData tc)
        {
            try
            {
                _lastPersistanceComplete = false;
                Configurator.Configuration.Persister.Persist(tc);
                _lastPersistanceComplete = true;
                lastPersistTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
                _lastPersistanceComplete = true;
            }
        }

        private static async Task MeasureAccumulator(ContainerBase tc)
        {
            try
            {
                ContainerBase trackerContainer = tc;
                if (_firstTC == null)
                {
                    _firstTC = trackerContainer;
                }
                else if (_firstTC == trackerContainer && !_trackerBlockCancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(15), _trackerBlockCancellationTokenSource.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        Configurator.Configuration.Logger.Info("Task.Delay exiting");
                    }
                }

                PostTrackers(trackerContainer, _trackerBlockCancellationTokenSource.IsCancellationRequested);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            finally
            {
                if (!_trackerBlockCancellationTokenSource.IsCancellationRequested)
                {
                    _trackerBlock.Post(tc);
                }
                else if (_trackerBlock.InputCount == 0)
                {
                    _trackerBlock.Complete();
                }
            }
        }

        private static void PostTrackers(ContainerBase trackerContainer, bool flushAll)
        {
            foreach (TrackerData td in
                trackerContainer.GetTrackerData(flushAll, Configurator.Configuration.Persister.PersistPreAggregatedBuckets))
            {
                _publisherBlock.Post(td);
            }
        }

        internal static void Register(ContainerBase trackerContainer)
        {
            _trackerBlock.Post(trackerContainer);
            _trackersRegisted = true;
        }

        internal static void ShutDown()
        {
            if (!_trackersRegisted)
                return;
            Thread.Sleep(500);
            _trackerBlockCancellationTokenSource.Cancel();
            Task.WaitAll(_trackerBlock.Completion);
            int loopCount = 0;
            while ((_publisherBlock.InputCount > 0 || !_lastPersistanceComplete) && loopCount < 20)
            {
                Thread.Sleep(100);
                loopCount++;
            }
            if (_publisherBlock.InputCount > 0)
                Configurator.Configuration.Logger.Error(
                    _publisherBlock.InputCount + " messages could not be persisted.",
                    new Exception("Graphene couldn't persist all message to persister."));
        }

        public static void FlushTrackers()
        {
            if (!_trackersRegisted)
                return;

            var loopCount = 0;
            var nextPersist = lastPersistTime.AddSeconds(196) - DateTime.UtcNow.AddSeconds(-30);
            Thread.Sleep(Math.Max(nextPersist.Milliseconds, 180000));
            PostTrackers(_firstTC, true);
            while ((_publisherBlock.InputCount > 0 || !_lastPersistanceComplete) && loopCount < 20)
            {
                Thread.Sleep(11000);
                loopCount++;
            }
        }
    }
}