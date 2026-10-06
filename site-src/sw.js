/* 瓶盖比价网 —— Service Worker「自毁版」（2026-10-01）
 *
 * 为什么自毁：
 *   原版 SW 会拦截商品库 data.js。上线后发现部分网络环境下它返回的内容
 *   不完整/损坏，导致 window.__DATA_PACK__ 未定义 ——
 *   页面首屏没有热词，搜索完全不出结果（手机、电脑都会复现，本地不走 SW 所以正常）。
 *   为了让已经注册过老版 SW 的用户自动恢复，这里保留文件但改成自毁逻辑：
 *     安装 -> 立即接管 -> 清空所有缓存 -> 注销自己 -> 让页面重载一次
 *   页面重载时本 SW 已注销，请求直接走网络，用户即恢复正常。
 *
 *   所以：不要再往这个文件里加任何缓存/拦截逻辑。
 */
self.addEventListener('install', function () {
  self.skipWaiting();
});

self.addEventListener('activate', function (e) {
  e.waitUntil(
    caches.keys()
      .then(function (keys) {
        return Promise.all(keys.map(function (k) { return caches.delete(k); }));
      })
      .then(function () { return self.registration.unregister(); })
      .then(function () { return self.clients.matchAll({ type: 'window' }); })
      .then(function (list) {
        list.forEach(function (c) {
          try { c.navigate(c.url); } catch (err) { /* 忽略 */ }
        });
      })
      .catch(function () { /* 忽略 */ })
  );
});

/* 不再拦截任何请求：即使被激活，也只是透传 */
self.addEventListener('fetch', function () { /* 不调用 respondWith，走默认网络请求 */ });
