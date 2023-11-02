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

package com.alibaba.polardbx.transaction.utils;

import org.apache.calcite.util.Pair;

import java.util.*;

/**
 * @author zhuangtianyi
 * Directed Graph data structure for deadlock detection.
 */
public class DiGraph<T> {
    // from edge -> to edges
    final private HashMap<T, ArrayList<T>> edges;

    public DiGraph() {
        this.edges = new HashMap<>();
    }

    public void addDiEdge(T from, T to) {
        assert from != null && to != null;
        this.edges.computeIfAbsent(from, (ignored) -> new ArrayList<>()).add(to);
    }

    private static class Detector<T> {
        final private HashMap<T, ArrayList<T>> edges;
        final private HashSet<T> discovered;
        final private HashSet<T> finished;
        private ArrayList<T> curPath; // 当前遍历路径上的节点

        Detector(HashMap<T, ArrayList<T>> edges) {
            this.edges = edges;
            this.discovered = new HashSet<>(); // 已经遍历过了的根节点(开始遍历)
            this.finished = new HashSet<>(); // 已经遍历完成的节点
        }

        private Optional<ArrayList<T>> detect() {
            for (T u : edges.keySet()) { // 遍历每个节点
                if (!discovered.contains(u) && !finished.contains(u)) { // 未遍历,未遍历完成
                    curPath = new ArrayList<>(1); // 当前路径
                    curPath.add(u); // 当前路径添加 当前节点
                    Optional<ArrayList<T>> result = dfs(u); // 深度遍历当前节点
                    if (result.isPresent()) {
                        return result;
                    }
                }
            }
            return Optional.empty();
        }

        private Optional<ArrayList<T>> dfs(T u) {
            discovered.add(u); // 当前节点已经遍历过了
            for (T v : Optional.ofNullable(edges.get(u)).orElse(new ArrayList<>())) { // 遍历当前节点的所有子节点
                if (discovered.contains(v)) { // 子节点已经遍历过了
                    return Optional.of(new ArrayList<>(curPath)); // 返回当前路径
                }
                if (!finished.contains(v)) { // 子节点未遍历完成
                    curPath.add(v); // 当前路径添加 子节点
                    Optional<ArrayList<T>> result = dfs(v); // 深度遍历子节点
                    curPath.remove(curPath.size() - 1); // 当前路径移除 子节点
                    if (result.isPresent()) { // 子节点有环
                        return result; // 返回当前路径
                    }
                }
            }
            discovered.remove(u); // 当前节点已经遍历完成
            finished.add(u); // 当前节点已经遍历完成
            return Optional.empty(); // 返回空
        }
    }

    /**
     * Detect cycle in graph.
     *
     * @return any cycle, or empty if no cycle detected.
     */
    public Optional<ArrayList<T>> detect() {
        return (new Detector<T>(this.edges).detect());
    }
}
