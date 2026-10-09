# Cost–Time Plan Optimization: hướng dẫn chạy

Quy trình gồm 3 phần, tương ứng 3 phần kết quả trong paper:

| Phần | Mục tiêu | Script | Output |
|---|---|---|---|
| 1. Model accuracy | Model ước lượng T, C đúng đến đâu | `cost_model_batch.py`, `cost_accuracy.py` | `cost-report/tpch100-v5/`, `.../accuracy/` |
| 2. Pareto front | Tìm bộ tham số (p_v, BF on/off) cân bằng time–cost | `plan_opt.py`, `plot_opt.py` | `cost-report/tpch100-v5/opt/` |
| 3. Hiệu năng | Chạy thật các plan đã chọn và so sánh | `benchmarks/gen-plans.py`, `plan_compare.py` | `history-plans/`, `.../opt/measured/` |

Mọi lệnh chạy từ thư mục gốc repo, dùng `.venv/bin/python`. Cách thu thập dữ liệu (probe, log, thư mục history)
được mô tả trong [`cost-model-data-collection.md`](cost-model-data-collection.md).

## 0. Dữ liệu đầu vào

Mỗi query cần có trong `history/<query>/`:

* các run **BF bật, S3** (`*--bloomfaas-s3-bloomfaas`),
* các run **BF tắt** (`*--starling-base`),
* (tuỳ chọn) các run **BF bật, NFS** (`*--bloomfaas-nfs-bloomfaas`), chỉ dùng để calibrate.

Các run được tạo bằng `benchmarks/run-seek15.py`: 5 seed × 3 run, `TASK_PARALLELISM = 70`. Run BF thứ i được ghép
với run no-BF thứ i (cùng seed).

## 1. Model accuracy

```bash
.venv/bin/python notebooks/cost_model_batch.py history --out notebooks/cost-report/tpch100-v5 \
    --serial --waves --disp-median --contention
.venv/bin/python notebooks/cost_accuracy.py notebooks/cost-report/tpch100-v5
```

* κ của mỗi query được calibrate trên các run của **các query khác** (leave-one-query-out).
* Nên dùng bộ flag ở trên. Lý do: Runner chạy các stage lần lượt (`--serial`), mỗi stage chạy p_v function qua
  `max_parallel` slot (`--waves`), và băng thông S3 bị chia giữa các function chạy đồng thời (`--contention`).
* Kết quả v5: MAPE của T là 17.7%, của C là 15.4%. Chart cho paper nằm trong `cost_accuracy_charts.ipynb`.

## 2. Pareto front: tìm p_v và BF on/off

### 2.1 Không gian tìm kiếm

Có hai loại tham số, đặt riêng cho từng stage:

| Tham số | Áp dụng cho | Giá trị |
|---|---|---|
| BF on/off | mỗi bước `merge_bf` (mỗi Bloom Filter) | bật / tắt |
| `max_size_mb` | mỗi stage `scan`, `aggregate`, `broadcast_join` | 10, 25, 50, 100, 200, 400, 1000 (mặc định 50) |

`max_size_mb` quyết định p_v, tức số function của stage, theo đúng cách Runner chia việc (`partition_tasks`):

* **scan:** chia các row group của bảng (lấy từ `benchmarks/metadata/tpch-100.json`) thành các nhóm có kích thước
  không quá `max_size_mb`, chỉ tính các cột được select.
* **combine (`aggregate`):** chia các file output của stage trước. Mỗi function của stage trước ghi ra 1 file.
* **`broadcast_join`:** p = số nhóm phía build × số nhóm phía probe.

### 2.2 Cách ước lượng một plan mà không cần chạy

Mỗi plan được ước lượng chỉ từ hai plan đã đo: bật hết BF và tắt hết BF.

1. **Chọn demand cho BF subset** (`plan_space.synth`). BF không làm đổi kết quả của join, nên tắt hay bật BF *j*
   chỉ ảnh hưởng tới các stage nằm giữa chỗ áp BF và join đích (gọi là vùng ảnh hưởng của *j*), cộng với các stage
   build, merge và apply của BF đó. Với mỗi stage:
   * mọi BF có vùng ảnh hưởng chứa stage này đều bật → lấy demand từ run BF;
   * không BF nào bật → lấy demand từ run no-BF;
   * chỉ một số BF bật → lấy demand từ run no-BF, nhân với hệ số lọc φ của các BF đang bật.
2. **Đổi p_v** (`plan_opt.resize`). Tổng số byte giữ nguyên. Số request, số invocation, số file BF đọc/ghi và mức
   tranh chấp tính theo p_v mới. Giá trị p_v mới được neo theo p_v đã đo ở mức `max_size_mb` mặc định.
3. **Ước lượng T và C** bằng cost model của phần 1 (cùng bộ flag v5), lấy trung vị trên 5 seed.

### 2.3 Tìm kiếm và chọn plan

* Coordinate descent trên hàm mục tiêu λ·T/T₀ + (1−λ)·C/C₀ với λ = 0, 0.1, …, 1. Mỗi λ chạy hai lần, xuất phát từ
  "bật hết BF" và từ "tắt hết BF". Ở đây T₀, C₀ là T, C của plan mặc định.
* Mọi plan đã đánh giá đều được giữ lại để dựng Pareto front.
* **Knee** là điểm trên front gần điểm lý tưởng (T_min, C_min) nhất, sau khi chuẩn hoá T, C theo giá trị nhỏ nhất.
* **Ràng buộc bộ nhớ:** plan nào có function đọc quá `--max-fn-mb` MB (mặc định 200) thì bị loại. Mức lớn nhất
  từng chạy được với 2 GiB là khoảng 163 MB.

### 2.4 Lệnh chạy

```bash
.venv/bin/python notebooks/plan_opt.py history --out notebooks/cost-report/tpch100-v5/opt          # cả 19 query, ~10 phút
.venv/bin/python notebooks/plan_opt.py history --queries q20 q21 --max-fn-mb 160                   # một số query, ràng buộc chặt hơn
.venv/bin/python notebooks/plot_opt.py notebooks/cost-report/tpch100-v5/opt --queries q5 q9 q20 q21
```

Output trong `cost-report/tpch100-v5/opt/`:

| File | Nội dung |
|---|---|
| `plans.json` | Với mỗi query có 4 plan: `default`, `knee`, `min_T`, `min_C`. Mỗi plan gồm `bf_off` (các bước `merge_bf` bị tắt), `max_size_mb` (chỉ các stage khác mặc định), `p` (p_v ước lượng), `T_est`, `C_est`, `max_fn_mb` |
| `points.csv` | Mọi plan đã đánh giá: T, C, có nằm trên front không, có phải knee / default không |
| `knee-vs-default.csv` | Bảng so sánh knee với default, dùng cho paper |
| `p-check.csv` | p_v tính lại so với p_v đo được, ở mức `max_size_mb` mặc định (v5: khớp 276/290 stage) |
| `pareto-opt.{pdf,png}` | Hình cho paper (4 query) |
| `pareto-opt-all.{pdf,png}` | Đủ 19 query (cho phụ lục) |

Hai bước trung gian dùng để giải thích trong paper:

```bash
# chỉ chọn BF subset (giữ p_v): front chỉ còn 1 điểm, cho thấy những BF vô dụng
.venv/bin/python notebooks/plan_space.py history --queries q2 q20 q21 --out notebooks/cost-report/tpch100-v5/plans
# BF subset kết hợp max_parallel toàn cục
.venv/bin/python notebooks/plan_space.py history --queries q5 q9 q20 q21 --parallel 5 10 20 30 50 70 100 \
    --out notebooks/cost-report/tpch100-v5/plans-mp
.venv/bin/python notebooks/plot_pareto.py notebooks/cost-report/tpch100-v5
```

## 3. Chạy thực nghiệm các plan đã chọn

### 3.1 Kiểm tra trước (không cần cluster)

```bash
.venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt/plans.json --dry-run --seeds seed1
```

Lệnh này sinh pipeline cho từng plan vào `benchmarks/plans/<query>-<plan>-<seed>.yml`, rồi kiểm tra ba điều: các
bước `merge_bf` bị tắt đã được xoá, `max_size_mb` đã được đặt đúng, và không còn dependency nào trỏ tới step không
tồn tại. Có thể so pipeline của một plan với pipeline gốc:

```bash
diff benchmarks/plans/q20-default-seed1.yml benchmarks/plans/q20-knee-seed1.yml
```

Khi tắt BF *j*, script làm ba việc:

* xoá bước `merge_bf` của *j*;
* ở các stage build BF, bỏ `bf_column`, `bf_path`, `est_elements`;
* ở các stage áp BF, bỏ `from_previous_task` (trỏ tới bước merge), `filter_by_bf`, `bf_column`.

Pipeline luôn chạy với `enable_bf=1` (hệ thống `bloomfaas-s3`), kể cả khi plan tắt hết BF. Nhờ vậy cấu hình scan
(4 worker) giống nhau giữa các plan.

### 3.2 Chạy

```bash
# nên chạy default và knee trước: 19 query × 2 plan × 5 seed × 3 run
.venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt/plans.json --plans default knee
# thêm hai đầu của front
.venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt/plans.json --plans min_T min_C
# một số query, ít run hơn
.venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt/plans.json --queries q20 q21 --runs 1
```

* Endpoint, `TASK_PARALLELISM` và `RUN_DELAY_S` được lấy từ `benchmarks/run-seek15.py`.
* Kết quả ghi vào `history-plans/<plan>/<query>/<timestamp>--bloomfaas-s3-bloomfaas/`, cùng định dạng với `history/`.
* Plan `default` được chạy lại trong cùng đợt với knee, để so sánh trong cùng điều kiện cluster.

### 3.3 Tổng hợp kết quả

```bash
.venv/bin/python notebooks/plan_compare.py history-plans \
    --plans-json notebooks/cost-report/tpch100-v5/opt/plans.json \
    --baseline history --out notebooks/cost-report/tpch100-v5/opt/measured
```

Output `plans-measured.csv` có một dòng cho mỗi cặp (query, plan), gồm:

* trung vị `T_meas`, `C_meas` trên các run;
* `T_est`, `C_est` lấy từ `plans.json`, và sai số `err_T_%`, `err_C_%` (dùng để kiểm chứng model ở những plan chưa
  từng chạy);
* `speedup_vs_default` và `saving_vs_default_%`.

`--baseline` thêm các run `starling-base` (no-BF) từ `history/` làm mốc so sánh. Hình `plans-measured.{pdf,png}`
là biểu đồ cột T và C cho từng query, mỗi plan một cột.

## 4. Hạn chế cần nêu trong paper

* **Chỉ có biến thể S3.** Plan NFS với BF tắt chưa từng được đo, nên chưa ghép được demand.
* **Ngoại suy.** Model chỉ được calibrate ở `max_size_mb` mặc định (khoảng 50 MB mỗi function) và
  `max_parallel` 50 hoặc 70. Các mức khác là ngoại suy; phần 3 dùng để kiểm chứng điều này (`err_T_%`, `err_C_%`).
* **Trường hợp chỉ bật một số BF:** demand của stage là no-BF nhân φ, chưa có số đo trực tiếp.
* **Baseline `starling-base` chạy scan với 1 worker,** còn các plan chạy với 4 worker. Vì vậy những stage lấy
  demand từ run no-BF có thể bị ước lượng hơi chậm.
* **Lỗi config đã biết:** `q21` / `step3-join-nation-supplier` khai báo `est_elements=1000` trong khi thực tế có
  khoảng 40 000 key. BF bị bão hoà (φ = 1). Optimizer tự tắt BF này.
