use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};

use flush::run_flush_memtable_job;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, RwLock, RwLockWriteGuard, Semaphore};
use trace::{error, info};

use crate::compaction::{flush, CompactTask, FlushReq, LevelCompactionPicker, Picker};
use crate::context::GlobalContext;
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;
use crate::TseriesFamilyId;

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

// 后台job就是通过检测该对象发现合并任务的
struct CompactProcessor {
    // bool 只是决定是否要先flush  但是在容器内的都要合并
    vnode_ids: HashMap<TseriesFamilyId, bool>,
}

impl CompactProcessor {
    fn insert(&mut self, vnode_id: TseriesFamilyId, should_flush: bool) {
        let old_should_flush = self.vnode_ids.entry(vnode_id).or_insert(should_flush);
        // 根据情况更新
        if should_flush && !*old_should_flush {
            *old_should_flush = should_flush
        }
    }

    // 取出此时所有节点的待处理状态
    fn take(&mut self) -> HashMap<TseriesFamilyId, bool> {
        std::mem::replace(&mut self.vnode_ids, HashMap::with_capacity(32))
    }
}

impl Default for CompactProcessor {
    fn default() -> Self {
        Self {
            vnode_ids: HashMap::with_capacity(32),
        }
    }
}

// 这个job更像是manager 管理所有合并任务
pub struct CompactJob {

    // 每次都是把需要被并发访问的东西抽出来 通过Arc包装
    inner: Arc<RwLock<CompactJobInner>>,
}

impl CompactJob {
    pub fn new(
        runtime: Arc<Runtime>,  // 通过它提交异步任务
        storage_opt: Arc<StorageOptions>,
        version_set: Arc<RwLock<VersionSet>>,  // 每个version针对的是一个vnode 这里要维护多个vode 也就是多张表
        global_context: Arc<GlobalContext>,
        summary_task_sender: Sender<SummaryTask>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CompactJobInner::new(
                runtime,
                storage_opt,
                version_set,
                global_context,
                summary_task_sender,
            ))),
        }
    }

    // 当接收到一个数据合并的任务时 记录在processor中 之后由后台线程执行
    pub async fn start_merge_compact_task_job(&self, compact_task_receiver: Receiver<CompactTask>) {
        self.inner
            .read()
            .await
            .start_merge_compact_task_job(compact_task_receiver);
    }

    // 开启后台扫描任务
    pub async fn start_vnode_compaction_job(&self) {
        self.inner.read().await.start_vnode_compaction_job();
    }

    // 设置enable_compaction 为false的同时  还返回了写锁
    pub async fn prepare_stop_vnode_compaction_job(&self) -> StartVnodeCompactionGuard {
        info!("StopCompactionGuard(create):prepare stop vnode compaction job");
        let inner = self.inner.write().await;
        inner
            .enable_compaction
            .store(false, atomic::Ordering::SeqCst);
        StartVnodeCompactionGuard { inner }
    }
}

impl std::fmt::Debug for CompactJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactJob").finish()
    }
}

struct CompactJobInner {
    runtime: Arc<Runtime>,
    storage_opt: Arc<StorageOptions>,
    version_set: Arc<RwLock<VersionSet>>,
    global_context: Arc<GlobalContext>,
    summary_task_sender: Sender<SummaryTask>,

    compact_processor: Arc<RwLock<CompactProcessor>>,  // 通过该对象记录需要合并的任务
    enable_compaction: Arc<AtomicBool>,  // 当前合并Job是否开启
    running_compactions: Arc<AtomicUsize>,  // 代表当前正在执行中的合并任务
}

// 通过该对象来维护和运行 合并任务
impl CompactJobInner {
    fn new(
        runtime: Arc<Runtime>,
        storage_opt: Arc<StorageOptions>,
        version_set: Arc<RwLock<VersionSet>>,
        global_context: Arc<GlobalContext>,
        summary_task_sender: Sender<SummaryTask>,
    ) -> Self {
        let compact_processor = Arc::new(RwLock::new(CompactProcessor::default()));

        Self {
            runtime,
            storage_opt,
            version_set,
            global_context,
            summary_task_sender,
            compact_processor,
            enable_compaction: Arc::new(AtomicBool::new(true)),
            running_compactions: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn start_merge_compact_task_job(&self, mut compact_task_receiver: Receiver<CompactTask>) {
        info!("Compaction: start merge compact task job");
        let compact_processor = self.compact_processor.clone();
        self.runtime.spawn(async move {
            while let Some(compact_task) = compact_task_receiver.recv().await {
                // Vnode id to compact & whether vnode be flushed before compact
                let (vnode_id, flush_vnode) = match compact_task {
                    // 热节点与冷节点的区别是  在合并数据前是否需要先flush
                    CompactTask::Vnode(id) => (id, false),
                    CompactTask::ColdVnode(id) => (id, true),
                };
                compact_processor
                    .write()
                    .await
                    .insert(vnode_id, flush_vnode);
            }
        });
    }

    // 这里只是开启后台扫描 而待合并任务是通过其他方法设置的
    fn start_vnode_compaction_job(&self) {
        info!("Compaction: start vnode compaction job");

        // 避免重复开启
        if self
            .enable_compaction
            .compare_exchange(
                false,
                true,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            info!("Compaction: enable_compaction is false, later to start vnode compaction job");
            return;
        }

        // 内部的组件都会被共享
        let runtime_inner = self.runtime.clone();
        let storage_opt = self.storage_opt.clone();
        let version_set = self.version_set.clone();
        let global_context = self.global_context.clone();
        let summary_task_sender = self.summary_task_sender.clone();
        let compact_processor = Arc::new(RwLock::new(CompactProcessor::default()));
        let enable_compaction = self.enable_compaction.clone();
        let running_compaction = self.running_compactions.clone();

        // 在异步运行时开启后台任务
        self.runtime.spawn(async move {
            // TODO: Concurrent compactions should not over argument $cpu.
            let compaction_limit = Arc::new(Semaphore::new(
                storage_opt.max_concurrent_compaction as usize,
            ));

            // 代表每隔多少秒 进入下一轮检测
            let mut check_interval =
                tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

            loop {
                check_interval.tick().await;
                // 发现合并功能已经被关闭了  退出循环
                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                    break;
                }

                // 后台任务是通过检查compact_processor 来判断是否有合并任务待执行的
                let processor = compact_processor.read().await;
                if processor.vnode_ids.is_empty() {
                    continue;
                }
                drop(processor);
                let vnode_ids = compact_processor.write().await.take();
                let vnode_ids_for_debug = vnode_ids.clone();
                let now = Instant::now();
                info!("Compacting on vnode(job start): {:?}", &vnode_ids_for_debug);
                for (vnode_id, flush_vnode) in vnode_ids {

                    // 获取该vnode相关的列族信息
                    let ts_family = version_set
                        .read()
                        .await
                        .get_tsfamily_by_tf_id(vnode_id)
                        .await;
                    if let Some(tsf) = ts_family {
                        info!("Starting compaction on ts_family {}", vnode_id);
                        let start = Instant::now();
                        // 首先检查当前列族 是否处于能够合并的状态  发现Running时 不能合并 那么就不存在读写文件时进行数据合并的问题了
                        if !tsf.read().await.can_compaction() {
                            info!("forbidden compaction on moving vnode {}", vnode_id);
                            return;
                        }

                        // 在设置了待合并的列族后  需要通过picker对象 挑选合适的数据文件
                        let picker = LevelCompactionPicker::new(storage_opt.clone());
                        let version = tsf.read().await.version();
                        // 挑选完成 产生req
                        let compact_req = picker.pick_compaction(version);
                        if let Some(req) = compact_req {
                            let database = req.database.clone();
                            let compact_ts_family = req.ts_family_id;
                            let out_level = req.out_level;

                            let ctx_inner = global_context.clone();
                            let version_set_inner = version_set.clone();
                            let summary_task_sender_inner = summary_task_sender.clone();

                            // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                            // 这个是合并的并行度
                            let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                            let enable_compaction = enable_compaction.clone();

                            let running_compaction = running_compaction.clone();
                            runtime_inner.spawn(async move {
                                // Check enable compaction
                                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                                    return;
                                }
                                // Edit running_compation
                                running_compaction.fetch_add(1, atomic::Ordering::SeqCst);
                                // 产生guard对象 在被回收时 自动减少 running_compaction
                                let _sub_running_compaction_guard = DeferGuard(Some(|| {
                                    running_compaction.fetch_sub(1, atomic::Ordering::SeqCst);
                                }));

                                // 热数据在合并前要先刷盘
                                if flush_vnode {
                                    let mut tsf_wlock = tsf.write().await;
                                    // 简单来看就是切换缓存
                                    tsf_wlock.switch_to_immutable();
                                    let flush_req = tsf_wlock.build_flush_req(true);
                                    drop(tsf_wlock);

                                    // 当产生刷盘请求后 就是执行刷盘任务
                                    if let Some(req) = flush_req {
                                        if let Err(e) = flush::run_flush_memtable_job(
                                            req,
                                            ctx_inner.clone(),
                                            version_set_inner,
                                            summary_task_sender_inner.clone(),
                                            None,  // 本身就是由compact触发的刷盘 所以不用传入sender
                                        )
                                        .await
                                        {
                                            error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                                        }
                                    }
                                }

                                // 此时统一完成了刷盘  开始进行合并任务
                                match super::run_compaction_job(req, ctx_inner).await {
                                    Ok(Some((version_edit, file_metas))) => {
                                        // TODO 统计相关
                                        metrics::incr_compaction_success();
                                        let (summary_tx, _summary_rx) = oneshot::channel();
                                        let _ = summary_task_sender_inner
                                            .send(SummaryTask::new(
                                                vec![version_edit],
                                                Some(file_metas),
                                                None,
                                                summary_tx,
                                            ))
                                            .await;

                                        metrics::sample_tskv_compaction_duration(
                                            database.as_str(),
                                            compact_ts_family.to_string().as_str(),
                                            out_level.to_string().as_str(),
                                            start.elapsed().as_secs_f64(),
                                        )
                                        // TODO Handle summary result using summary_rx.
                                    }
                                    Ok(None) => {
                                        info!("There is nothing to compact.");
                                    }
                                    Err(e) => {
                                        metrics::incr_compaction_failed();
                                        error!("Compaction job failed: {:?}", e);
                                    }
                                }
                                drop(permit);
                            });
                        }
                    }
                }
                info!(
                    "Compacting on vnode(job start): {:?} costs {} sec",
                    vnode_ids_for_debug,
                    now.elapsed().as_secs()
                );
            }
        });
    }

    pub async fn prepare_stop_vnode_compaction_job(&self) {
        self.enable_compaction
            .store(false, atomic::Ordering::SeqCst);
    }

    async fn wait_stop_vnode_compaction_job(&self) {
        let mut check_interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            check_interval.tick().await;
            if self.running_compactions.load(atomic::Ordering::SeqCst) == 0 {
                return;
            }
        }
    }
}

pub struct StartVnodeCompactionGuard<'a> {
    inner: RwLockWriteGuard<'a, CompactJobInner>,
}

impl<'a> StartVnodeCompactionGuard<'a> {
    pub async fn wait(&self) {
        info!("StopCompactionGuard(wait): wait vnode compaction job to stop");
        self.inner.wait_stop_vnode_compaction_job().await
    }
}

impl<'a> Drop for StartVnodeCompactionGuard<'a> {
    fn drop(&mut self) {
        info!("StopCompactionGuard(drop): start vnode compaction job");
        self.inner.start_vnode_compaction_job();
    }
}

pub struct DeferGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f()
        }
    }
}

// 代表一次刷盘任务
pub struct FlushJob {
    runtime: Arc<Runtime>,
    version_set: Arc<RwLock<VersionSet>>,
    global_context: Arc<GlobalContext>,

    // 执行flush请求的过程中可能会产生2个task  通过sender对象发往下游
    summary_task_sender: Sender<SummaryTask>,  // 通过该对象往下发送 SummaryTask (总结任务)
    compact_task_sender: Sender<CompactTask>,  // 通过该对象往下发送 CompactTask (数据合并任务)
}

impl FlushJob {
    pub fn new(
        runtime: Arc<Runtime>,
        version_set: Arc<RwLock<VersionSet>>,
        global_context: Arc<GlobalContext>,
        summary_task_sender: Sender<SummaryTask>,
        compact_task_sender: Sender<CompactTask>,
    ) -> Self {
        Self {
            runtime,
            version_set,
            global_context,
            summary_task_sender,
            compact_task_sender,
        }
    }

    // 启动异步任务 监听flush请求
    pub fn start_vnode_flush_job(&self, mut flush_req_receiver: Receiver<FlushReq>) {
        let runtime_inner = self.runtime.clone();
        let version_set = self.version_set.clone();
        let global_context = self.global_context.clone();
        let summary_task_sender = self.summary_task_sender.clone();
        let compact_task_sender = self.compact_task_sender.clone();
        self.runtime.spawn(async move {
            while let Some(x) = flush_req_receiver.recv().await {
                // TODO(zipper): this make config `flush_req_channel_cap` wasted
                // 每当收到一个flush请求后 执行flushJob
                // Run flush job and trigger compaction.
                runtime_inner.spawn(run_flush_memtable_job(
                    x,
                    global_context.clone(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    Some(compact_task_sender.clone()),
                ));
            }
        });
        info!("Flush task handler started");
    }
}

impl std::fmt::Debug for FlushJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushJob").finish()
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{self, AtomicI32};
    use std::sync::Arc;

    use super::DeferGuard;
    use crate::compaction::job::CompactProcessor;
    use crate::TseriesFamilyId;

    #[test]
    fn test_build_compact_batch() {
        let mut compact_batch_builder = CompactProcessor::default();
        compact_batch_builder.insert(1, false);
        compact_batch_builder.insert(2, false);
        compact_batch_builder.insert(1, true);
        compact_batch_builder.insert(3, true);
        assert_eq!(compact_batch_builder.vnode_ids.len(), 3);
        let mut keys: Vec<TseriesFamilyId> =
            compact_batch_builder.vnode_ids.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(compact_batch_builder.vnode_ids.get(&1), Some(&true));
        assert_eq!(compact_batch_builder.vnode_ids.get(&2), Some(&false));
        assert_eq!(compact_batch_builder.vnode_ids.get(&3), Some(&true));
        let vnode_ids = compact_batch_builder.take();
        assert_eq!(vnode_ids.len(), 3);
        assert_eq!(vnode_ids.get(&1), Some(&true));
        assert_eq!(vnode_ids.get(&2), Some(&false));
        assert_eq!(vnode_ids.get(&3), Some(&true));
    }

    #[test]
    fn test_defer_guard() {
        let a = Arc::new(AtomicI32::new(0));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        {
            let a = a.clone();
            let jh = runtime.spawn(async move {
                a.fetch_add(1, atomic::Ordering::SeqCst);
                let _guard = DeferGuard(Some(|| {
                    a.fetch_sub(1, atomic::Ordering::SeqCst);
                }));
                a.fetch_add(1, atomic::Ordering::SeqCst);
                a.fetch_add(1, atomic::Ordering::SeqCst);

                assert_eq!(a.load(atomic::Ordering::SeqCst), 3);
            });
            let _ = runtime.block_on(jh);
        }
        assert_eq!(a.load(atomic::Ordering::SeqCst), 2);
    }
}
