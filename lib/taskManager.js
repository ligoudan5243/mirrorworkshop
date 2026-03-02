// lib/taskManager.js
const BATCH_SIZE = 50;

// 辅助函数：更新活动任务列表
async function updateActiveTasks(env, taskId, action, taskData = null) {
    const key = 'active_tasks';
    let tasks = await env.B2_KV.get(key, 'json') || [];
    
    if (action === 'add') {
        // 防止重复添加
        if (!tasks.find(t => t.taskId === taskId)) {
            tasks.push(taskData);
        }
    } else if (action === 'remove') {
        tasks = tasks.filter(t => t.taskId !== taskId);
    }
    
    await env.B2_KV.put(key, JSON.stringify(tasks));
}

export async function createMasterTask(env, taskId, owner, repo, bucketId, filePaths) {
    const batches = [];
    for (let i = 0; i < filePaths.length; i += BATCH_SIZE) {
        batches.push(filePaths.slice(i, i + BATCH_SIZE));
    }

    const masterTask = {
        taskId,
        owner,
        repo,
        bucketId,
        totalFiles: filePaths.length,
        totalBatches: batches.length,
        completedBatches: [],
        processedFiles: 0,
        failedFiles: [],
        currentFile: null,
        status: 'processing',
        createdAt: Date.now(),
    };
    await env.B2_KV.put(`master:${taskId}`, JSON.stringify(masterTask));

    // 添加到活动任务列表
    await updateActiveTasks(env, taskId, 'add', {
        taskId,
        name: `${owner}/${repo}`,
        totalFiles: filePaths.length,
        processedFiles: 0,
        progress: 0,
        status: 'processing'
    });

    for (let i = 0; i < batches.length; i++) {
        const batchTask = {
            type: 'batch',
            masterTaskId: taskId,
            bucketId,
            owner,
            repo,
            files: batches[i],
            batchIndex: i,
            totalBatches: batches.length,
        };
        await env.TASKS_QUEUE.send(JSON.stringify(batchTask));
    }
    return masterTask;
}

// 任务完成或失败时，从活动列表中移除
export async function completeMasterTask(env, taskId, finalStatus) {
    const key = `master:${taskId}`;
    const task = await env.B2_KV.get(key, 'json');
    if (task) {
        task.status = finalStatus;
        task.completedAt = Date.now();
        await env.B2_KV.put(key, JSON.stringify(task));
    }
    // 从活动列表移除
    await updateActiveTasks(env, taskId, 'remove');
}

// 更新任务进度时，同时更新活动列表中的进度
export async function updateMasterTaskProgress(env, taskId, updates) {
    const key = `master:${taskId}`;
    const task = await env.B2_KV.get(key, 'json') || {};
    Object.assign(task, updates, { updatedAt: Date.now() });
    await env.B2_KV.put(key, JSON.stringify(task));

    // 同时更新活动列表中的进度（可选，可以实时显示进度）
    const activeTasks = await env.B2_KV.get('active_tasks', 'json') || [];
    const index = activeTasks.findIndex(t => t.taskId === taskId);
    if (index !== -1) {
        activeTasks[index].processedFiles = task.processedFiles || 0;
        activeTasks[index].progress = task.completedBatches ? Math.round((task.completedBatches.length / task.totalBatches) * 100) : 0;
        activeTasks[index].currentFile = task.currentFile;
        await env.B2_KV.put('active_tasks', JSON.stringify(activeTasks));
    }
}
