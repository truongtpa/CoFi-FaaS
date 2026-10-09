# Thu thập dữ liệu cho Cost Model (Section "Cost Modeling" trong `paper/main.tex`)

Mục tiêu: chạy BLOOM-FaaS như bình thường cho hai trường hợp, **bật BF** (`enable_bf=1`) và **tắt BF** (`enable_bf=0`).
Mỗi function invocation tự ghi lại các đại lượng mà cost model cần. Sau khi chạy xong, script
`notebooks/cost_model.py` áp các công thức của paper lên dữ liệu đo được và cho biết model lệch ở đâu, lệch bao nhiêu.

## 1. Kiến trúc (không đổi logic hiện tại)

| Thành phần | File | Cách gắn vào code |
|---|---|---|
| Probe phía function | `operations/libs/CostProbe.py` | `operations/__init__.py`: `install(app)`. Dùng Flask `before_request` / `after_request` và bọc (wrap) các hàm I/O trong `ParquetS3Funcs`. Response giữ nguyên, chỉ thêm khóa `cost_probe` ở top-level (cạnh `data`). |
| Log phía runner | `operations/libs/CostLog.py` | `Runner._prepare_execute_context` gọi `CostLog.dag(...)`; `Pipeline.execute_and_save` gọi `CostLog.stage(...)`. Mọi lỗi đều bị bắt lại, không làm dừng pipeline. |
| Phân tích | `notebooks/cost_model.py` | Chạy offline trên thư mục history. |

Tắt probe: đặt env `COST_PROBE=0` trong `deploy.yaml` (function sẽ trả response như cũ).

Các function đọc `data` của response như trước (`Pipeline.load_json`), nên việc thêm `cost_probe` không ảnh hưởng tới các step phía sau.

## 2. Đại lượng thu thập cho mỗi invocation (`cost_probe`)

| Khóa | Ý nghĩa | Ký hiệu trong paper |
|---|---|---|
| `t_total` | Thời gian xử lý phía server (từ `before_request` đến `after_request`) | $d_{v,i}$ (chưa gồm dispatch) |
| `t_read` | Thời gian các lần đọc tường minh: `read_parquet_with_retry(_column)` (S3), `load_bloom_filter` | $t^{\mathcal I}_{v,i}$ |
| `t_write` | Thời gian upload S3 (`upload_to_s3_with_retry`) và `save_bloom_filter` | $t^{\mathcal O}_{v,i}$ |
| `t_comp` | `t_total - t_read - t_write` | $t^{\mathcal C}_{v,i}$ |
| `in.{s3,nfs}.bytes/req` | Bytes (nén) của các column chunk đã đọc và số request ước lượng, tách theo storage class | $D^{\mathrm{in}}_{v,i,s}, Q^{\mathrm{in}}_{v,i,s}$ |
| `out.{s3,nfs}.bytes/req` | Kích thước file output thật (stat file) và số request ghi | $D^{\mathrm{out}}_{v,i,s}, Q^{\mathrm{out}}_{v,i,s}$ |
| `rows_in`, `rows_out` | Số dòng đầu vào (theo metadata parquet) và đầu ra | $\lvert R\rvert, \lvert S\rvert$, output cardinality |
| `rows_build`, `rows_probe` | Số dòng phía build/probe của join | dùng để ước lượng $\sigma^{\ltimes}_j$ |
| `bf` | `role` = `build` / `merge` / `apply`, `column`, `est_elements`, `error_rate` | $n_j$, $f_j$ thiết kế |
| `bf_read`, `bf_write` | Số lần và số bytes đọc/ghi file BF | $R_j$, $X_j$ |
| `cpu`, `mem` | Giới hạn CPU/bộ nhớ đọc từ cgroup của pod | $c_v, m_v$ |
| `cold`, `seq`, `uptime_s` | Request đầu tiên của process (cold start), số thứ tự request, thời gian từ lúc process khởi động | $\kappa^{\mathrm{cold}}$ |
| `host` | Pod xử lý request | |

Phía runner bổ sung thêm cho mỗi task: `lat` (thời gian client đo), `disp = lat - t_total` (overhead invoke/dispatch/mạng, gồm cả cold start của pod), `start`, `end`, `retries`.

### Quy ước ước lượng request

* Đọc parquet: `Q = 2 + số column chunk được đọc` (HEAD/footer + một range GET cho mỗi column chunk).
  Với scan: chỉ tính các cột được select (giống `parse_select_columns`); `scan-starling` đọc toàn bộ cột.
  Với join/aggregate/hash-partition: tính mọi cột của các file input.
* Ghi: 1 request cho mỗi file output / bucket / file BF.
* Đọc BF: 1 request cho mỗi file `.bin`.

## 3. File được ghi trong thư mục history của mỗi lần chạy

```
history/<query>/<timestamp>--<system>/
  cost-dag.json        # DAG: các step, func, deps, tham số BF, enable_bf, max_parallel
  cost-stages.jsonl    # mỗi dòng là một stage: p, wall_s, danh sách task kèm toàn bộ cost_probe
  <Step>.json          # như cũ; response của mỗi task có thêm khóa cost_probe
```

## 4. Cách chạy

1. Build lại image function (`build.sh`) để có `CostProbe`, rồi deploy như bình thường.
2. Chạy cùng một query hai lần, một lần với `enable_bf=1` và một lần với `enable_bf=0` (ví dụ bằng `benchmarks/run*.py`).
3. Phân tích:

```bash
python notebooks/cost_model.py history/q3/<run-bf> history/q3/<run-nobf> --out notebooks/cost-report/q3
python notebooks/cost_model.py history/q3/<run-bf> history/q3/<run-nobf> --params my-params.json --calib nobf
```

Chạy toàn bộ history (hiệu chỉnh leave-one-query-out: κ của mỗi query lấy từ mọi run của các query còn lại):

```bash
python notebooks/cost_model_batch.py history --out notebooks/cost-report/tpch100          # --calib nobf để hiệu chỉnh theo cặp
```

Cost Model Accuracy theo từng query (đọc lại các `report.json` ở trên, không chạy lại model): bảng sai số T/C, MAPE, decision match, hình sai số và scatter model vs đo, xuất vào `<out>/accuracy/`. Notebook tương ứng: `notebooks/cost_accuracy.ipynb`.

```bash
.venv/bin/python notebooks/cost_accuracy.py notebooks/cost-report/tpch100
```

Các cờ của `cost_model_batch.py` (mặc định đều tắt, tức công thức ban đầu):

| Cờ | Ý nghĩa |
|---|---|
| `--per-instance` | $d_v=\max_i d_{v,i}$ từ $D_{v,i}, Q_{v,i}$ của từng function; $C^{\mathcal C}$ dùng $\sum_i d_{v,i}$ |
| `--merge-work` | thêm $W^{\mathrm{bf-merge}}=p^b_j m_j$, tốc độ hiệu chỉnh trên `merge_bf` |
| `--serial` | `Runner.execute` chạy các stage tuần tự theo thứ tự pipeline: $T=\sum_v d_v$ (RunnerDAG mới chạy song song các nhánh) |
| `--waves` | mỗi stage chạy qua pool `max_parallel`: $d_v$ = makespan của $p_v$ function trên `max_parallel` slot |
| `--rel-fit` | hiệu chỉnh tốc độ compute theo sai số tương đối (không để task lớn lấn át stage nhỏ) |
| `--disp-median` | $\kappa^{\mathrm{disp}}$ = trung vị thay vì trung bình (cold start kéo trung bình lên) |
| `--contention` | các function chạy cùng lúc chia băng thông storage: thời gian mỗi byte $=1/\kappa^{\mathrm{bw}}_s+\beta_s k$, $k$ = số function đồng thời (đo được khi hiệu chỉnh, $\min(p_v,\texttt{max\_parallel})$ khi dự đoán); áp cho I/O S3 tường minh và phần đọc trong DuckDB của join/aggregate |
| `--orch` | cộng $\kappa^{\mathrm{orch}}$ (khoảng trống đo được giữa stage trước và stage sau) |

`tpch100-v3` = `--serial --waves --rel-fit --disp-median`. `tpch100-v4` = `--serial --waves --disp-median --contention` (không dùng `--rel-fit` cùng `--contention`: hiệu chỉnh theo sai số tương đối kéo mọi dự đoán xuống thấp).

`--calib nobf` (mặc định của `cost_model.py`): hiệu chỉnh $\kappa$ trên run không bật BF rồi dùng để dự đoán cả hai run, nên sai lệch ở run có BF cho thấy phần mà model của BF dự đoán chưa đúng.
`--calib bf|both`: hiệu chỉnh trên run có BF / cả hai. `--calib-dirs d1 d2 ...`: hiệu chỉnh trên các run khác.

Ví dụ `my-params.json` (các khóa thiếu sẽ lấy giá trị mặc định):

```json
{
  "bw":  {"nfs": 300e6},
  "lat": {"nfs": 0.0005},
  "r_hash": 8e7,
  "mem_gb": 2,
  "calibrate_storage": ["s3"],
  "price": {"gbs": 16.6667e-6, "inv": 0.2e-6, "get": {"s3": 0.4e-6}, "put": {"s3": 5e-6}}
}
```

## 5. Script áp công thức như thế nào

| Bước | Công thức (paper) | Cách tính trong `cost_model.py` |
|---|---|---|
| $\kappa^{\mathrm{bw}}_s, \kappa^{\mathrm{lat}}_s/\kappa^{\mathrm{con}}_s$ | Eq. input/output time | Hồi quy bình phương tối thiểu `t = D/bw + Q·lat` chỉ trên các I/O được đo tường minh: S3 = đọc của scan (`t_read`) và upload (`t_write`); NFS = `load/save_bloom_filter` trong `merge_bf`. |
| $\kappa^{\mathrm{cpu}}$ theo operator ($W_{op}/\rho_{op}$) | Eq. compute demand | Theo khóa `endpoint@mode` (`mode` = `nfs` nếu task đọc/ghi dữ liệu trên NFS): hồi quy `t_comp − t_bf = D_data/(c·rate) + Q_impl·qlat`. `D_data` không gồm bytes BF; `Q_impl` là số request DuckDB tự đọc (join/aggregate), nên phần đọc bên trong DuckDB và phần ghi NFS được hấp thụ vào `rate`/`qlat` thay vì áp băng thông S3 đơn luồng. |
| $\kappa^{\mathrm{disp}}$ | | Trung bình `disp` |
| Công việc BF | $W^{\mathrm{bf-build}}=k\lvert B\rvert$, $W^{\mathrm{bf-apply}}=k\lvert P\rvert$ | `k = round(-log2 error_rate)`, $\lvert B\rvert$ = `rows_out` của stage build, $\lvert P\rvert$ = `rows_in` của scan apply, tốc độ `r_hash` |
| $d_v$ | $\max_i(t^{\mathcal I}+t^{\mathcal C}+t^{\mathcal O}+\kappa^{\mathrm{cold}}+\kappa^{\mathrm{disp}})$ | Model: chia đều (balanced) $D/p$. Đo: `meas_d_fn` = max của `t_total+disp`, `meas_d_wall` = thời gian thật của stage |
| $T$ | Critical path $\sum_{v\in P^*} d_v$ | `T_model`; so với `T_meas` (thời gian thật của query), `T_cp_wall`, `T_cp_fn` |
| $C^{\mathcal I}_v$ | $\sum_s Q^{\mathrm{in}}_{v,s}\pi^{\mathrm{get}}_s$ | Q đo được |
| $C^{\mathcal C}_v$ | $p_v\pi^{\mathrm{inv}} + p_v d_v (m_v\pi^{\mathrm{gbs}} + c_v\pi^{\mathrm{cpus}})$ | Model: $p_v d_v$; đo: $\sum_i$ `t_total` |
| $C^{\mathcal O}_v$ | $\sum_s Q^{\mathrm{out}}\pi^{\mathrm{put}} + D^{\mathrm{out}} t^{\mathrm{ret}}\pi^{\mathrm{sto}}$ | $t^{\mathrm{ret}}$ đo = thời điểm query kết thúc − thời điểm stage kết thúc; model lấy theo lịch EFT |
| $C^{\mathrm{net}}$ | $X\pi^{\mathrm{net}}$ | $X$ = bytes vào/ra S3 |
| $R_j$ | $2p^b_j + 1 + p^p_j$ | So với số lần đọc/ghi BF đếm thực tế |
| $X_j$ | $\frac{m_j}{8}R_j$ | So với tổng bytes BF thực tế |
| $f_j$ | $(1-e^{-kn/m})^k$ | $m$ = kích thước BF sau merge × 8, $n$ = số key thực sự được chèn |
| $\sigma^{\ltimes}_j$ | Eq. semijoin | Từ run không BF: join đầu tiên trong DAG có một input chứa stage build BF và input kia chứa scan áp BF; `σ = rows_out(join) / rows_out(input phía scan)`. Không dùng `rows_build/rows_probe` vì broadcast join nhân chúng lên p lần. Không có join như vậy thì σ = NaN (cột `no_semijoin`). |
| $\varphi_j$ | $\sigma^{\ltimes} + (1-\sigma^{\ltimes})f$ | So với $\varphi$ đo = `rows_out(scan, BF) / rows_out(scan, không BF)`, kèm `f_implied` |
| $D^{\mathrm{out}}$ của scan có BF | $\varphi_j D^{\mathrm{in}}$ | `D_out_model = φ_model · D_out(không BF)` so với `D_out` đo |
| Quyết định BF | $T^{(1)}<T^{(0)}$ và $C^{(1)}<C^{(0)}$ | `decision_match`: model và thực đo có cùng kết luận hay không |

## 6. Kết quả đầu ra

* Màn hình, cho mỗi run: bảng theo stage gồm các pha (`meas_t_*` so với `model_t_*`), `meas_d_fn`, `meas_d_wall`, `model_d`, `err_d_%`; bảng I/O và chi phí (`meas_C`, `model_C`, `err_C_%`); tổng `T` và `C`.
* Phần BF, cho mỗi `merge_bf`: `R`, `X`, `m`, `k`, `n`, `f`, và cho mỗi scan apply: $\sigma^{\ltimes}$, $\varphi$ đo/model, `D_out` đo/model, `delta_comp_cpu_s` (chênh lệch CPU-giây giữa BF và không BF).
* `<out>/stages-bf.csv`, `<out>/stages-nobf.csv`, `<out>/report.json`.

Cách đọc sai lệch:

* `meas_d_wall` ≫ `meas_d_fn`: có hàng đợi, vì runner chỉ chạy tối đa `max_parallel` task cùng lúc, trong khi model giả sử $p_v$ function chạy song song.
* `meas_d_fn` ≫ `model_d`: tải giữa các function lệch nhau (model giả sử balanced) hoặc có cold start (`cold > 0`, `disp` lớn).
* `T_meas` > `T_cp_wall`: có overhead điều phối giữa các stage của runner.
* `err_phi_%` lớn: $\sigma^{\ltimes}$ hoặc $f$ bị ước lượng sai (so sánh `f_implied` với `f_model`, `n_act` với `n_est`).
* `R_meas ≠ R_model`: bố cục BF thực tế khác với giả định (build $p^b$ → merge 1 → probe $p^p$).

## 7. Giới hạn đã biết

* Với các stage đọc input bằng DuckDB `read_parquet` (join, aggregate, hash-partition), phần đọc nằm lẫn trong `t_comp`, không tách riêng được; `t_read` chỉ gồm các lần đọc tường minh (scan từ S3, file BF).
* `hash_partition` upload bucket trong thread pool riêng nên thời gian ghi nằm trong `t_comp`; bytes và request ghi thì vẫn được đếm.
* `bloom_load` / `bloom_build` chạy bên trong extension DuckDB, nên thời gian của chúng nằm trong `t_comp`.
* Việc lấy metadata input/output diễn ra sau khi đã đo `t_total`, nên không làm sai `t_total`; nhưng nó làm `lat` phía client tăng thêm vài ms (vài request metadata tới S3), và phần này bị tính vào `disp`.
* `disp` gồm mạng, định tuyến Knative, hàng đợi và thời gian khởi động pod; không tách riêng được thời gian khởi động pod.
