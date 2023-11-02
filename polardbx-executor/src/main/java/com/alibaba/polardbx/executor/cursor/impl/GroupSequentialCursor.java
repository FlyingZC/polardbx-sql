/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.GenericPhyObjectRecorder;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class GroupSequentialCursor extends AbstractCursor {

    protected String schemaName;
    protected Map<String, List<RelNode>> plansByInstance;
    protected int totalSize = 0;
    protected final ExecutionContext executionContext;

    protected Cursor currentCursor = null;
    protected int currentIndex = 0;
    protected List<Cursor> cursors = new ArrayList<>();
    protected final ReentrantLock cursorLock = new ReentrantLock();

    protected AtomicBoolean started = new AtomicBoolean(false); // 这条物理记录执行开始
    protected AtomicInteger numObjectsDone = new AtomicInteger(0); // 执行完成,物理成功失败
    protected AtomicInteger numObjectsSkipped = new AtomicInteger(0); // 执行跳过
    protected BlockingQueue<Cursor> completedCursorQueue;

    protected static final int INTERRUPT_TIMEOUT = 10 * 1000;
    protected static final int EMPTY_QUEUE_CHECK_TIMEOUT = 60 * 1000;

    protected List<Throwable> exceptionsWhenCloseSubCursor = new ArrayList<>();
    protected List<Throwable> exceptions;

    public GroupSequentialCursor(Map<String, List<RelNode>> plansByInstance, ExecutionContext executionContext,
                                 String schemaName, List<Throwable> exceptions) {
        super(false);
        this.schemaName = schemaName;
        this.plansByInstance = plansByInstance;
        for (List<RelNode> plans : this.plansByInstance.values()) {
            this.totalSize += plans.size();
        }
        this.completedCursorQueue = new LinkedBlockingQueue<>(this.totalSize);
        this.executionContext = executionContext;
        this.exceptions = Collections.synchronizedList(exceptions);

        RelNode plan0 = this.plansByInstance.values().iterator().next().get(0);
        this.returnColumns = ((BaseTableOperation) plan0).getCursorMeta().getColumns();
    }

    @Override
    public void doInit() {
        if (this.inited) {
            return;
        }
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        String traceId = executionContext.getTraceId();
        for (List<RelNode> plans : plansByInstance.values()) { // 获取该实例上所有的执行计划
            // Inter-instance in parallel and intra-instance sequentially
            executionContext.getExecutorService().submit(schemaName, traceId, AsyncTask.build(() -> {
                // Execute sequentially on the same instance
                executeSequentially(plans); // 具体执行
            }));
        }

        super.doInit();
    }

    private void executeSequentially(List<RelNode> plans) {
        int numObjectsCountedOnInstance = 0;
        int delay = executionContext.getParamManager().getInt(ConnectionParams.EMIT_PHY_TABLE_DDL_DELAY);
        if (delay > 0) {
            try {
                Thread.sleep(delay * 1000);
            } catch (InterruptedException e) {
            }
        }
        for (RelNode plan : plans) {
            GenericPhyObjectRecorder phyObjectRecorder =
                CrossEngineValidator.getPhyObjectRecorder(plan, executionContext); // 每一条物理sql,对应一条物理执行日志.创建物理执行记录者.记录是否执行成功

            try {
                started.set(true);

                if (!phyObjectRecorder.checkIfDone()) {
                    Cursor cursor = ExecutorContext.getContext(schemaName)
                        .getTopologyExecutor()
                        .execByExecPlanNode(plan, executionContext); // 执行物理sql

                    phyObjectRecorder.recordDone(); // 记录执行结果,执行后日志

                    cursorLock.lock();
                    try {
                        cursors.add(cursor);
                        if (returnColumns == null) {
                            returnColumns = cursors.get(0).getReturnColumns();
                        }
                    } finally {
                        cursorLock.unlock();
                    }

                    completedCursorQueue.put(cursor);
                    numObjectsDone.incrementAndGet(); // 统计执行结果
                    numObjectsCountedOnInstance++;
                } else { // 已经执行完成的,直接记录数量
                    numObjectsSkipped.incrementAndGet(); // 跳过数量++
                    numObjectsCountedOnInstance++; // 执行数量++
                }
            } catch (Throwable t) {
                try {
                    if (!phyObjectRecorder.checkIfIgnoreException(t)) {
                        exceptions.add(t);
                    }
                } catch (Throwable checkError) {
                    exceptions.add(checkError);
                }

                numObjectsSkipped.incrementAndGet();
                numObjectsCountedOnInstance++;

                if (CrossEngineValidator.isJobInterrupted(executionContext)) {
                    // Skip the rest of objects.
                    numObjectsSkipped.addAndGet(plans.size() - numObjectsCountedOnInstance);
                    // Don't continue anymore since the job has been cancelled.
                    return;
                }
            }

        }
    }

    @Override
    public Row doNext() {
        init();

        Row ret;

        long cursorStartTime = System.currentTimeMillis();
        long elapsedTimeBeforeStart;
        long emptyQueueStartTime = System.currentTimeMillis();
        long emptyQueueDuration;

        while (true) {
            if ((numObjectsDone.get() + numObjectsSkipped.get() == totalSize) && currentIndex >= numObjectsDone.get()) {
                return null;
            }

            if (numObjectsDone.get() == 0 && numObjectsSkipped.get() == 0 && !started.get()) {
                elapsedTimeBeforeStart = System.currentTimeMillis() - cursorStartTime;
                if (elapsedTimeBeforeStart >= INTERRUPT_TIMEOUT) {
                    // The DDL job is very likely to have been interrupted and
                    // never proceed, so we have to terminate.
                    throwException();
                }
            }

            if (currentCursor == null) {
                try {
                    currentCursor = completedCursorQueue.poll(DdlConstants.MEDIAN_WAITING_TIME, TimeUnit.MILLISECONDS);
                    if (currentCursor == null) {
                        emptyQueueDuration = System.currentTimeMillis() - emptyQueueStartTime;
                        if (emptyQueueDuration > EMPTY_QUEUE_CHECK_TIMEOUT &&
                            CrossEngineValidator.isJobInterrupted(executionContext)) {
                            // The job has been cancelled/interrupted, so we have to terminate this
                            // to avoid occupying the job scheduler permanently in some scenarios.
                            throwException();
                        }
                        // Try again
                        continue;
                    }
                    emptyQueueStartTime = System.currentTimeMillis();
                } catch (InterruptedException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "The DDL job has been interrupted");
                } catch (Throwable t) {
                    throw GeneralUtil.nestedException(t);
                }
                RuntimeStatHelper.registerCursorStatByParentCursor(executionContext, this, currentCursor);
            }

            ret = currentCursor.next();
            if (ret != null) {
                return ret;
            }

            switchCursor();
        }
    }

    protected void switchCursor() {
        currentCursor.close(exceptionsWhenCloseSubCursor);
        currentIndex++;
        currentCursor = null;
    }

    private void throwException() {
        DdlContext ddlContext = executionContext.getDdlContext();
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INTERRUPTED, String.valueOf(ddlContext.getJobId()),
            ddlContext.getSchemaName(), ddlContext.getObjectName());
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        exs.addAll(exceptionsWhenCloseSubCursor);
        for (Cursor cursor : cursors) {
            if (cursor != null) {
                exs = cursor.close(exs);
            }
        }

        cursors.clear();
        return exs;
    }

    public Cursor getCurrentCursor() {
        return currentCursor;
    }
}
