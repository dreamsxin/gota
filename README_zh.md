Gota：Go 语言的 DataFrame、Series 与数据处理库
==================================================

`github.com/dreamsxin/gota` — Go 1.24.9+

English: [README.md](README.md)

Gota 是一个可嵌入、单进程、纯内存的 DataFrame / Series / 数据处理（data
wrangling）Go 实现，设计受 pandas 启发。Gota 提供即时求值的 Go 风格 API，
而非与 pandas API 一一对应。API 仍在演进，升级前请阅读发布说明。

**v2 已发布：** `v2/` 目录承载列式内核模块 `github.com/dreamsxin/gota/v2`
（标签 `v2.0.0`）——v2 API 文档与性能报告见 **[v2/README.md](v2/README.md)**，
升级指南见 [v2/MIGRATION.md](v2/MIGRATION.md)，设计文档见
[v2/docs/rfc-columnar-kernel.md](v2/docs/rfc-columnar-kernel.md)。v2 按 Go
主版本子目录布局开发，与本文档描述的 API 是干净断开：移除了
`Element`/`Elem` 接口和 `Rapply` 家族，改用类型化的 Series 访问器与列式
apply；Excel/Parquet/SQL 适配器迁移至 `v2/excel`、`v2/parquet`、`v2/sql`
子模块。本 README 描述 v1.x 线，该线仅接收缺陷修复。

## 目录

- [安装](#安装)
- [DataFrame](#dataframe)
  - [数据加载](#数据加载)
  - [获取行数据](#获取行数据)
  - [子集与切片](#子集与切片)
  - [列选择](#列选择)
  - [Schema（结构描述）](#schema结构描述)
  - [更新值](#更新值)
  - [过滤](#过滤)
  - [分组、聚合、Apply 与 Transform](#分组聚合apply-与-transform)
  - [透视表 Pivot](#透视表-pivot)
  - [排序 Arrange](#排序-arrange)
  - [列变换 Mutate](#列变换-mutate)
  - [连接 Join](#连接-join)
  - [拼接 Concat](#拼接-concat)
  - [函数应用](#函数应用)
  - [累计统计](#累计统计)
  - [差分与变化率](#差分与变化率)
  - [滚动窗口与指数加权](#滚动窗口与指数加权)
  - [带策略和上限的缺失值填充](#带策略和上限的缺失值填充)
  - [相关与协方差](#相关与协方差)
  - [宽转长 Melt](#宽转长-melt)
  - [Excel 读写](#excel-读写)
  - [Parquet 读写](#parquet-读写)
  - [SQL 读写](#sql-读写)
  - [索引与多级索引](#索引与多级索引)
- [链式操作](#链式操作)
- [错误处理](#错误处理)
- [保存到文件](#保存到文件)
- [打印到控制台](#打印到控制台)
- [与 gonum 互操作](#与-gonum-互操作)
- [数据探索](#数据探索)
- [缺失数据处理](#缺失数据处理)
- [值操作](#值操作)
- [管道 Pipe](#管道-pipe)
- [Series](#series)
  - [基本用法](#基本用法)
  - [缺失填充 FillNaN](#缺失填充-fillnan)
  - [带上限的前向/后向填充](#带上限的前向后向填充)
  - [滚动窗口](#滚动窗口)
  - [指数加权移动平均 EWM](#指数加权移动平均-ewm)
  - [累计统计（Series）](#累计统计series)
  - [差分与变化率（Series）](#差分与变化率series)
  - [相关与协方差（Series）](#相关与协方差series)
  - [类型转换](#类型转换)
  - [分类类型 Categorical](#分类类型-categorical)
  - [字符串操作](#字符串操作)
  - [时间分量访问器](#时间分量访问器)
- [更多 DataFrame API（v1.2.1）](#更多-dataframe-apiv121)
  - [Shift 平移](#shift-平移)
  - [Assign 计算新列](#assign-计算新列)
  - [Explode 展开](#explode-展开)
  - [Query 表达式查询](#query-表达式查询)
  - [Stack / Unstack](#stack--unstack)
  - [Resample 重采样](#resample-重采样)
  - [并行操作](#并行操作)
- [更多 I/O API（v1.2.1）](#更多-io-apiv121)
  - [JSON Lines（NDJSON）](#json-linesndjson)
  - [Excel — 工作表选择](#excel--工作表选择)
  - [SQL — 占位符风格](#sql--占位符风格)
  - [CSV 流式读取](#csv-流式读取)
- [许可证](#许可证)

---

## 安装

```bash
go get github.com/dreamsxin/gota
```

要求 Go 1.24.9+。主要依赖：

| 包 | 用途 |
|---|---|
| `gonum.org/v1/gonum` | 数值计算 |
| `github.com/xuri/excelize/v2` | Excel 读写（无 CGO） |
| `github.com/parquet-go/parquet-go` | Parquet 读写 |
| `modernc.org/sqlite` | SQL 测试（纯 Go SQLite） |
| `github.com/olekukonko/tablewriter` | 表格格式化输出 |

注意：Excel、Parquet、SQL 适配器位于核心 `dataframe` 包内，因此即使你只使用
内存操作或 CSV/JSON，这些依赖也会出现在 `go.mod` 中。Go 只会编译你实际导入
的代码，但模块图和下载量会包含全部依赖；拆分适配器需要 API 破坏性变更，已
推迟到 v2（见 ROADMAP 的设计决策）。

---

DataFrame
---------

DataFrame 是二维表格数据集：列代表特征，行代表观测。列保持类型完整性，并支持
NaN（缺失）值。

### 数据加载

直接从 Series 构造：

```go
df := dataframe.New(
    series.New([]string{"b", "a"}, series.String, "COL.1"),
    series.New([]int{1, 2}, series.Int, "COL.2"),
    series.New([]float64{3.0, 4.0}, series.Float, "COL.3"),
)
```

从 `[][]string` 记录加载：

```go
df := dataframe.LoadRecords(
    [][]string{
        {"A", "B", "C", "D"},
        {"a", "4", "5.1", "true"},
        {"k", "5", "7.0", "true"},
        {"k", "4", "6.0", "true"},
        {"a", "2", "7.1", "false"},
    },
)
```

从结构体切片加载：

```go
type User struct {
    Name     string
    Age      int
    Accuracy float64
    ignored  bool // 未导出字段会被忽略
}
users := []User{
    {"Aram", 17, 0.2, true},
    {"Juan", 18, 0.8, true},
    {"Ana", 22, 0.5, true},
}
df := dataframe.LoadStructs(users)
```

显式指定类型配置：

```go
df := dataframe.LoadRecords(
    records,
    dataframe.DetectTypes(false),
    dataframe.DefaultType(series.Float),
    dataframe.WithTypes(map[string]series.Type{
        "A": series.String,
        "D": series.Bool,
    }),
)
```

从 `[]map[string]interface{}` 加载：

```go
df := dataframe.LoadMaps(
    []map[string]interface{}{
        {"A": "a", "B": 1, "C": true, "D": 0},
        {"A": "b", "B": 2, "C": true, "D": 0.5},
    },
)
```

从 CSV / JSON 读取器加载：

```go
df := dataframe.ReadCSV(strings.NewReader(csvStr))
df := dataframe.ReadJSON(strings.NewReader(jsonStr))

// 自动推断逗号、制表符、分号或竖线分隔符。
df := dataframe.ReadCSV(strings.NewReader(tsvOrCsv), dataframe.DetectDelimiter(true))
```

从 HTML 表格加载：

```go
dfs := dataframe.ReadHTML(r) // 返回 []DataFrame，每个表格一个
```

### 获取行数据

```go
row := df.GetRow(0) // map[string]interface{}
```

### 子集与切片

```go
sub := df.Subset([]int{0, 2})       // 按索引取行
sub := df.SliceRow(1, 4)            // 行区间 [1, 4)（左闭右开）
```

### 列选择

```go
sel1 := df.Select([]int{0, 2})
sel2 := df.Select([]string{"A", "C"})
dropped := df.Drop([]string{"B"})
```

**`Col` 返回副本。** `df.Col("x")` 返回该列的独立副本——修改它永远不会写回
DataFrame，也不会报错：

```go
col := df.Col("x")
col.Set([]int{0}, series.New([]int{9}, series.Int, "x")) // 静默丢弃

// 要修改列，应在 DataFrame 上替换：
df = df.Mutate(series.New([]int{9, 1}, series.Int, "x"))  // 按列名替换
df = df.Assign("x2", func(d dataframe.DataFrame) series.Series { ... })
df = df.Set([]int{0}, replacementFrame)                    // 单元格级修改
```

### Schema（结构描述）

`Schema` 是有序的列布局：列名、物理类型与可空性。可用于检查
join/concat 兼容性，或构造布局一致的空帧作为输出缓冲。

```go
sch := df.Schema()

sch.Names()  // []string{"A", "B"}
sch.Types()  // []series.Type{series.String, series.Int}
sch.Field("A")         // (Field, bool)
sch.Equal(otherSchema) // 快速布局兼容性检查

// 相同布局的零行帧（流式累加器、输出缓冲）
empty := dataframe.FromSchema(sch)
```

v1.x 的所有列都是可空的（`Field.Nullable` 恒为 `true`）；保留该字段是为了
将来引入非可空存储时无需 API 变更。

### 更新值

```go
df2 := df.Set(
    []int{0, 2},
    dataframe.LoadRecords(
        [][]string{
            {"A", "B", "C", "D"},
            {"b", "4", "6.0", "true"},
            {"c", "3", "6.0", "false"},
        },
    ),
)
```

### 过滤

```go
// OR 过滤（默认）
fil := df.Filter(
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)

// 显式 OR
fil := df.FilterAggregation(dataframe.Or,
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"B", series.Greater, 4},
)

// AND 过滤
fil := df.FilterAggregation(dataframe.And,
    dataframe.F{"A", series.Eq, "a"},
    dataframe.F{"D", series.Eq, true},
)
```

内置比较器：`Eq`、`Neq`、`Greater`、`GreaterEq`、`Less`、`LessEq`、`In`、`Out`。

使用 `series.CompFunc` 自定义比较器：

```go
hasPrefix := func(prefix string) func(series.Element) bool {
    return func(el series.Element) bool {
        if val, ok := el.Val().(string); ok {
            return strings.HasPrefix(val, prefix)
        }
        return false
    }
}
fil := df.Filter(dataframe.F{"A", series.CompFunc, hasPrefix("aa")})
```

### 分组、聚合、Apply 与 Transform

```go
groups := df.GroupBy("key1", "key2")
aggre  := groups.Aggregation(
    []AggregationType{Aggregation_MAX, Aggregation_MIN},
    []string{"values", "values2"},
)
```

**Apply** — 对每个分组应用任意函数（类似 `pandas groupby().apply()`）：

```go
result := df.GroupBy("category").Apply(func(g dataframe.DataFrame) dataframe.DataFrame {
    return g.Capply(func(s series.Series) series.Series {
        return series.Floats(s.Mean())
    })
})
```

**Transform** — 按组变换列，结果对齐原始行序：

```go
groups := df.GroupBy("category")
transformed, err := groups.Transform("value", func(s series.Series) series.Series {
    mean := s.Mean()
    vals := s.Float()
    out := make([]float64, len(vals))
    for i, v := range vals {
        out[i] = v - mean
    }
    return series.Floats(out...)
})
```

**GetGroups** — 访问底层分组映射：

```go
groupMap := groups.GetGroups() // map[string]DataFrame
```

### 透视表 Pivot

```go
pivot := df.Pivot(
    []string{"A", "B"},   // 行键
    []string{"C", "D"},   // 列键
    []PivotValue{
        {Colname: "E", AggregationType: Aggregation_SUM},
        {Colname: "F", AggregationType: Aggregation_COUNT},
    },
)
```

### 排序 Arrange

```go
sorted := df.Arrange(
    dataframe.Sort("A"),    // 升序
    dataframe.RevSort("B"), // 降序
)
```

### 列变换 Mutate

```go
// 替换或新增列
mut := df.Mutate(series.New([]string{"a", "b", "c", "d"}, series.String, "C"))
```

### 连接 Join

`InnerJoin`、`LeftJoin`、`RightJoin`、`OuterJoin`、`CrossJoin`：

```go
join := df.InnerJoin(df2, "D")
```

### 拼接 Concat

支持两个方向的变参拼接；单帧会被复制，粘性错误会传播：

```go
all := dataframe.Concat(a, b, c)         // 纵向；缺失列补 NaN
wide := dataframe.ConcatColumns(a, b, c) // 横向；CBind 语义
```

### 函数应用

```go
mean := func(s series.Series) series.Series {
    floats := s.Float()
    sum := 0.0
    for _, f := range floats { sum += f }
    return series.Floats(sum / float64(len(floats)))
}
df.Capply(mean) // 按列
df.Rapply(mean) // 按行
```

### 累计统计

```go
cumDF  := df.CumSum()              // 滚动求和，作用于全部数值列
cumProd := df.CumProd("price", "qty") // 仅指定列
```

### 差分与变化率

```go
diffDF := df.Diff(1)                        // row[i] - row[i-1]
pct    := df.PctChange(2, "close", "volume") // 2 期变化率
```

### 滚动窗口与指数加权

DataFrame 级构建器与 Series API 对应。不指定列时，所有数值列被变换，其余列
原样保留；显式列清单必须全部是数值列，否则报错（`ErrTypeMismatch`、
`ErrColumnNotFound`，可用 `errors.Is` 判断）。

```go
rolled := df.Rolling(3).Mean()               // 全部数值列
rolled := df.Rolling(3).StdDev("close", "volume")

smooth := df.EWM(3).Mean()                   // span 约定与 pandas 一致
smooth := df.EWMAlpha(0.5).Mean("close")     // 显式 alpha
vol    := df.EWM(3).Std("close")
```

滚动统计：`Mean`、`Sum`、`Min`、`Max`、`StdDev`。EWM 统计：`Mean`、`Var`、`Std`。

### 带策略和上限的缺失值填充

```go
// 前向填充，最多连续填 2 个 NaN
filled := df.FillNAStrategyLimit(dataframe.NAFillForward, 2)

// 后向填充无上限（0 = 无限制），仅指定列
filled := df.FillNAStrategyLimit(dataframe.NAFillBackward, 0, "col1", "col2")
```

另有：`df.FillNAStrategy(strategy, subset...)`（无上限）、
`df.DropNA(how, subset...)` 删除含缺失值的行、
`df.DropDuplicates(subset...)` 删除重复行。
`FillNaNStrategy` / `FillNaNStrategyLimit` 是与 Series 侧 `FillNaN` 命名
一致的拼写兼容别名。

### 相关与协方差

返回方阵，行列名与原数值列一致：

```go
corrMatrix := df.Corr() // 皮尔逊相关系数矩阵
covMatrix  := df.Cov()  // 样本协方差矩阵
```

### 宽转长 Melt

```go
long := df.Melt(
    []string{"id", "date"},                    // 标识列
    []string{"open", "high", "low", "close"},  // 值列（留空 = 其余全部）
    "field",                                   // 变量列名
    "value",                                   // 值列名
)
```

### Excel 读写

使用 [excelize](https://github.com/xuri/excelize)，无需 CGO。

```go
// 读
df := dataframe.ReadXLSX(r)
df := dataframe.ReadXLSXFile("data.xlsx",
    dataframe.HasHeader(true),
    dataframe.WithTypes(map[string]series.Type{"price": series.Float}),
)
sheets, err := dataframe.ReadXLSXSheets(r) // map[string]DataFrame，每表一个

// 写
err := df.WriteXLSX(w)
err := df.WriteXLSXFile("output.xlsx")
err := df.WriteXLSX(w,
    dataframe.WithXLSXBoldHeader(true),
    dataframe.WithXLSXColumnWidths(map[string]float64{"name": 18}),
    dataframe.WithXLSXNumberFormats(map[string]string{"amount": "#,##0.00"}),
)
```

### Parquet 读写

使用 [parquet-go](https://github.com/parquet-go/parquet-go)。支持 Gota 的
`String`、`Int`、`Float`、`Bool`、`Time` 列。缺失值存为 Parquet null，写入
按有界行批处理。

```go
err := df.WriteParquet(w)
err := df.WriteParquetFile("data.parquet")

df := dataframe.ReadParquet(readerAt, size)
df := dataframe.ReadParquetFile("data.parquet")
```

### SQL 读写

**FromSQL** — 从 `*sql.Rows` 构造 DataFrame：

```go
rows, _ := db.Query("SELECT id, name, score FROM users WHERE active = 1")
df := dataframe.FromSQL(rows)
```

**WriteSQL** — 插入数据库表：

```go
err := df.WriteSQL(db, "users",
    dataframe.WithCreateTable(true),   // CREATE TABLE IF NOT EXISTS
    dataframe.WithTruncateFirst(true), // 插入前 DELETE FROM
    dataframe.WithBatchSize(200),      // 每条 INSERT 的行数（默认 500）
)

// SQLite / PostgreSQL 按唯一键或主键 upsert。
err := df.WriteSQL(db, "users",
    dataframe.WithUpsert("id"),
    dataframe.WithUpsertUpdateColumns("name", "score"),
)
```

SQL ↔ Series 类型映射：

| SQL 类型 | Series 类型 |
|---|---|
| INT / INTEGER / BIGINT … | `series.Int` |
| REAL / FLOAT / DOUBLE … | `series.Float` |
| BOOL / BOOLEAN | `series.Bool` |
| DATE / DATETIME / TIMESTAMP | `series.Time` |
| 其他 | `series.String` |

### 索引与多级索引

**单级索引**

```go
idx := dataframe.NewIndex([]string{"a", "b", "c", "d"})
idf, err := df.WithIndex(idx)

rows := idf.Loc("b")           // 标签为 "b" 的所有行
rows := idf.LocSlice("a", "c") // 标签切片（两端包含）

// 用某列作为索引（该列被移出数据）
idf, err := df.WithColumnIndex("id")

// 还原为普通 DataFrame
plain := idf.ResetIndex("id")
```

**多级索引**

```go
mi, err := dataframe.NewMultiIndex(
    []string{"2024", "2024", "2025", "2025"}, // 第 0 级
    []string{"Q1",   "Q2",   "Q1",   "Q2"},   // 第 1 级
)
midf, err := df.WithMultiIndex(mi)

rows := midf.Loc("2024", "Q1") // 完整键
rows := midf.Loc("2024")       // 部分键（2024 的全部行）
```

`IndexedDataFrame` 和 `MultiIndexedDataFrame` 是查询包装器。`Loc` 等操作返回
普通 `DataFrame`；在 v1.2.1 中，索引不会在任意 DataFrame 变换间自动传播。

### 链式操作

大多数变换方法返回新的 DataFrame。`DataFrame.Set`、`DataFrame.FillNaN`、
`DataFrame.SetNames` 会修改共享的列存储；`Series.Set`、`Series.Append`、
`Series.FillNaN` 同样会修改原 Series。若需要保留原值，请先调用 `Copy`。

`NewNoCopy` 共享输入 Series 的元素存储，但不共享 Series 头：就地元素写入
（`Series.Set`）会透过 DataFrame 可见，而 `Series.Append` 会重新赋值切片头，
**不会**可见。安全规则：把 Series 交给 `NewNoCopy` 后就不要再碰它。

返回 DataFrame 的操作会传播粘性错误：一旦出错，后续链式操作都变成空操作，
直到错误被检查：

```go
a = a.Rename("Origin", "Country").
    Filter(dataframe.F{"Age", series.Less, 50}).
    Filter(dataframe.F{"Origin", series.Eq, "United States"}).
    Select([]string{"Id", "Origin", "Date"}).
    Subset([]int{1, 3})
if a.Err != nil {
    log.Fatal(a.Err)
}
```

### 错误处理

常见错误会包装导出的哨兵错误，因此 `errors.Is` 可用，且错误文案保持稳定：

```go
import "errors"

col := df.Col("missing")
if errors.Is(col.Err, dataframe.ErrColumnNotFound) {
    // 处理列缺失
}

sel := df.Select([]string{"missing"})
if errors.Is(sel.Err, dataframe.ErrColumnNotFound) { /* ... */ }
```

可用哨兵包括 `ErrEmptyDataFrame`、`ErrColumnNotFound`、
`ErrIndexOutOfRange`、`ErrLengthMismatch`、`ErrKeyNotFound`（连接键与索引
标签）、`ErrEmptyKeys`、`ErrInvalidAggregation`。

### 保存到文件

```go
file, _ := os.Create("output.csv")
defer file.Close()
df.WriteCSV(file)

df.WriteJSON(w)
```

### 打印到控制台

```go
fmt.Println(flights)

> [336776x20] DataFrame
>
>     X0    year  month day   dep_time sched_dep_time dep_delay arr_time ...
>  0: 1     2013  1     1     517      515            2         830      ...
>  ...
```

### 与 gonum 互操作

```go
type matrix struct{ dataframe.DataFrame }

func (m matrix) At(i, j int) float64  { return m.Elem(i, j).Float() }
func (m matrix) T() mat.Matrix        { return mat.Transpose{m} }
```

加载 `gonum/mat.Matrix`：

```go
df := dataframe.LoadMatrix(mat)
```

---

### 数据探索

#### 头尾预览

```go
df.Head(5)   // 前 5 行
df.Tail(10)  // 后 10 行
```

#### 描述性统计

```go
df.Describe() // 汇总统计（计数、均值、标准差、最小、最大、四分位）
```

#### 结构信息

```go
df.Info(os.Stdout)
// 输出维度、列类型、非空计数、内存估算
```

#### 值计数

```go
vc := df.ValueCounts("category", false, false) // 计数，降序
vc := df.ValueCounts("category", true, false)  // 占比
```

#### Top N

```go
top10   := df.NLargest(10, "revenue")
bottom5 := df.NSmallest(5, "price")
```

#### 随机抽样

```go
sample := df.Sample(100, -1, false, 42)   // 100 行，固定种子
sample := df.Sample(-1, 0.1, false, 42)   // 10% 的行
sample := df.Sample(1000, -1, true, 42)   // 有放回
```

---

### 缺失数据处理

```go
mask := df.IsNull()  // 或 df.IsNA()  — 值为 NaN 处为 true
mask := df.NotNull() // 或 df.NotNA() — 值存在处为 true

// 删除子集列中含任意 NaN（或全部 NaN）的行
df2 := df.DropNA(dataframe.NAHowAny, "col1", "col2")
df2 := df.DropNA(dataframe.NAHowAll) // 仅删除所有列都是 NaN 的行

// 删除重复行
df2 := df.DropDuplicates("key1", "key2")
```

---

### 值操作

#### 截断 Clip

```go
lower, upper := 0.0, 100.0
df2 := df.Clip(&lower, &upper)                    // 全部数值列
df2 := df.ClipColumn("discount", &lower, &upper)  // 单列
```

#### 替换 Replace

```go
df2 := df.Replace("N/A", nil)                        // 整个 DataFrame
df2 := df.ReplaceInColumn("status", "unknown", nil)  // 单列
```

#### 批量改类型 Astype

```go
df2 := df.Astype(map[string]series.Type{
    "price":  series.Float,
    "qty":    series.Int,
    "active": series.Bool,
})
```

#### 区间判断 Between / IsIn

```go
mask := df.Between("age", 18, 65, "both") // "both"|"neither"|"left"|"right"
mask := df.IsIn("country", []interface{}{"US", "UK", "CA"})
df2  := df.FilterIsIn("country", []interface{}{"US", "UK", "CA"})
```

### 管道 Pipe

```go
result := df.
    Filter(dataframe.F{"age", series.Greater, 18}).
    Pipe(customTransform).
    Arrange(dataframe.Sort("name"))

// 带额外参数
result := df.PipeWithArgs(customFunc, arg1, arg2)

// 逐元素 map
df2 := df.ApplyMap(func(val interface{}) interface{} {
    if s, ok := val.(string); ok {
        return strings.ToUpper(s)
    }
    return val
})
```

---

Series
------

Series 是支持 NaN 的类型化向量，是 DataFrame 列的构成单元。

支持的类型：`Int`、`Float`、`String`、`Bool`、`Time`

### 基本用法

```go
s := series.New([]string{"b", "a"}, series.String, "COL.1")

// 便捷构造器
series.Strings(values)
series.Ints(values)
series.Floats(values)
series.Bools(values)
series.Times(values)
```

核心方法：`Len`、`Elem`、`Val`、`Float`、`Int`、`Int64`、`Bool`、`Records`、
`Copy`、`Subset`、`Set`、`Append`、`Concat`、`Slice`、`Map`、`Order`、`Unique`、
`NUnique`、`ValueCounts`、`HasNaN`、`IsNaN`、`FillNaN`、`Compare`、`Empty`。

统计：`Mean`、`StdDev`、`Median`、`Min`、`Max`、`MinStr`、`MaxStr`、
`Sum`、`Quantile`。

NaN 行为：
- `nil` 值和字符串 `"NaN"` 视为缺失
- `Int(math.Inf(...))` → NaN；`Float(math.NaN())` → NaN 元素
- 比较运算符（`Eq`、`Less` 等）在任一操作数为 NaN 时总返回 `false`
- `Bool` 只接受 `0/1`、`true/false`、`t/f` — 其他值变为 NaN

### 缺失填充 FillNaN

```go
s := series.New([]interface{}{"a", "b", nil}, series.String, "COL.1")
s.FillNaN(series.Strings("c"))
s.FillNaNForward()   // ffill：向前传播最近的有效值
s.FillNaNBackward()  // bfill：向后传播下一个有效值
```

### 带上限的前向/后向填充

```go
s := series.New([]interface{}{1.0, nil, nil, nil, 5.0}, series.Float, "x")

s.FillNaNForwardLimit(1)  // → [1, 1, NaN, NaN, 5]（最多填 1 个空隙）
s.FillNaNBackwardLimit(0) // → [1, 5, 5, 5, 5]（0 = 无限制）
```

### 滚动窗口

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")

s.Rolling(3).Mean()
s.Rolling(3).MinPeriods(1).Mean() // 至少有 1 个观测即输出
s.Rolling(3).Sum()
s.Rolling(3).Min()   // O(n) 单调双端队列算法
s.Rolling(3).Max()   // O(n) 单调双端队列算法
s.Rolling(3).StdDev() // 贝塞尔校正（ddof=1）
s.Rolling(3).Apply(func(w []float64) float64 {
    return w[len(w)-1] - w[0]
})
```

默认 `minPeriods` 等于窗口大小 — 窗口不满的前导位置输出 NaN。用
`MinPeriods(1)` 可在只有一个观测时就开始输出。

### 指数加权移动平均 EWM

与 `pandas.ewm()` 接口一致。`alpha = 2 / (span + 1)`。

```go
s := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "price")

s.EWM(3).Mean()               // adjusted 模式（pandas 默认），span=3
s.EWMAlpha(0.5).Mean()        // 直接指定 alpha（等价 span=3）
s.EWM(3).Adjust(false).Mean() // 递推模式：y[i] = α·x[i] + (1-α)·y[i-1]
s.EWM(3).MinPeriods(2).Mean() // 有效观测不足 2 个时输出 NaN
s.EWM(3).IgnoreNA(true).Mean()
s.EWM(3).Var()  // 指数加权方差（ddof=1）
s.EWM(3).Std()  // Var 的平方根
```

| 模式 | 公式 |
|---|---|
| `Adjust(true)`（默认） | `Σ (1-α)^k · x[i-k] / Σ (1-α)^k` |
| `Adjust(false)` | `y[i] = α·x[i] + (1-α)·y[i-1]` |

### 累计统计（Series）

```go
s.CumSum()  // [1, 3, 6, 10, 15]
s.CumProd() // [1, 2, 6, 24, 120]
s.CumMax()  // 滚动最大值
s.CumMin()  // 滚动最小值
```

NaN 会传播：一旦出现 NaN，其后所有值也是 NaN。

### 差分与变化率（Series）

```go
s := series.New([]float64{10, 12, 15, 11}, series.Float, "close")

s.Diff(1)      // [NaN, 2, 3, -4]
s.Diff(2)      // [NaN, NaN, 5, -1]
s.PctChange(1) // [NaN, 0.20, 0.25, -0.267]
```

`PctChange` 除以 `abs(prev)`，前值为 0 时返回 NaN。

### 相关与协方差（Series）

NaN 对会被排除。有效对不足 2 个时返回 `NaN`。

```go
x := series.New([]float64{1, 2, 3, 4, 5}, series.Float, "x")
y := series.New([]float64{2, 4, 6, 8, 10}, series.Float, "y")

corr := x.Corr(y) // 1.0（皮尔逊）
cov  := x.Cov(y)  // 5.0（样本协方差，ddof=1）
```

### 类型转换

基于 Go 泛型（Go 1.18+）的通用批量转换。直接分配、不复用池，返回的
Series 安全地持有自己的内存。

```go
// 泛型 — 适用于任意源切片类型
s := series.BatchConvert([]int{1, 2, 3}, series.Float, "values")

// 类型化便捷函数
s := series.BatchConvertInts([]int{1, 2, 3}, series.Float, "values")
s := series.BatchConvertFloats([]float64{1.5, 2.5}, series.String, "values")
s := series.BatchConvertStrings([]string{"1", "2", "invalid"}, series.Int, "values")
s := series.BatchConvertBools([]bool{true, false}, series.Int, "values")
```

转换规则：
- 非法字符串 → NaN（如 `"abc"` 转 Int）
- `int/int64` → `time.Time` 用 `time.Unix(v, 0)`
- `string` → `time.Time` 要求 RFC3339 格式，其他变 NaN

### 分类类型 Categorical

`Categorical` 是低基数率字符串数据（国家代码、状态标签、枚举型列）的独立、
省内存表示。采用字典编码：排序后的唯一字符串切片 + `[]int32` 编码数组。
v1.2.1 中它不是原生 `Series.Type`；放入 DataFrame 前请先 `ToSeries`。

```go
// 从字符串切片创建
cat := series.NewCategorical([]string{"US", "UK", "US", "DE"}, "country")

// 与普通 String Series 互转
cat, err := series.CategoricalFromSeries(s)
s := cat.ToSeries()

// 查看
cat.Len()          // 行数
cat.NCategories()  // 不同取值数
cat.Categories()   // 排序后的字典切片
cat.Get(i)         // 第 i 行的字符串值
cat.IsNA(i)        // 第 i 行是否缺失

// 频次计数
counts := cat.ValueCounts() // map[string]int

// 修改
cat.AddCategory("FR")          // 扩展字典
cat.SetValue(0, "FR")          // 设置行值（必须在字典中）

// 过滤
filtered, err := cat.Filter([]bool{true, false, true, false})

// 内存估算
bytes := cat.MemoryBytes()
```

### 字符串操作

变换类操作返回新的 String Series（NaN 保持 NaN）；谓词类返回 Bool Series。
对非 String Series 调用会设置 Err。

```go
s := series.New([]string{"Go", " py ", nil}, series.String, "lang")

s.Upper()               // ["GO", " PY ", NaN]
s.Lower()
s.TrimSpace()           // ["Go", "py", NaN]
s.Trim("G")
s.TrimPrefix("G")
s.TrimSuffix(" ")
s.ReplaceAll("o", "0")  // 子串替换；Series.Replace 是值替换

s.Contains("o")         // [true, false, NaN]
s.StartsWith("G")
s.EndsWith(" ")
```

### 时间分量访问器

每个访问器从 Time Series 返回 Int Series（NaN 保持 NaN）；`Weekday` 遵循
`time.Weekday` 编号（周日 = 0）。对非 Time Series 调用会设置 Err。

```go
t := series.New(
    []interface{}{time.Date(2024, 3, 5, 14, 30, 45, 0, time.UTC), nil},
    series.Time, "ts",
)

t.Year()    // [2024, NaN]
t.Month()   // [3, NaN]
t.Day()     // [5, NaN]
t.Hour()    // [14, NaN]
t.Minute()
t.Second()
t.Weekday() // [2, NaN]（周二）
```

---

### 更多 DataFrame API（v1.2.1）

#### Shift 平移

```go
df.Shift(1)           // 所有列下移 1 行（顶部补 NaN）
df.Shift(-2, "price") // "price" 列上移 2 行（底部补 NaN）
```

#### Assign 计算新列

```go
df2 := df.Assign("profit", func(d dataframe.DataFrame) series.Series {
    rev := d.Col("revenue").Float()
    cost := d.Col("cost").Float()
    out := make([]float64, len(rev))
    for i := range rev { out[i] = rev[i] - cost[i] }
    return series.Floats(out)
})
```

#### Explode 展开

```go
// "tags" 列："go,python" → 两行
df2 := df.Explode("tags")
```

#### Query 表达式查询

```go
df.Query("age > 18")
df.Query("status == active")
df.Query("age >= 18 AND age <= 65")
df.Query("country in US,UK,CA")
df.Query("score > 0.5 OR label == good")
df.Query("active == true AND (score > 0.5 OR label == good)")
df.Query(`label in "A AND B","x,y"`) // 引号值可含空格/逗号
```

运算符：`==`、`!=`、`>`、`>=`、`<`、`<=`、`in`、`not in`。
用 `AND` / `OR` 组合（不区分大小写）。包含运算符子串的列名（如 `income`、
`bandwidth`）能正确处理。`AND` 优先级高于 `OR`；括号可改变优先级。
单引号或双引号值可包含逻辑词和逗号。

#### Stack / Unstack

```go
// 宽 → 长（Melt 的别名）
long := df.Stack([]string{"id"}, []string{"q1","q2","q3"}, "quarter", "value")

// 长 → 宽
wide := df.Unstack([]string{"id"}, "quarter", "value")
```

#### Resample 重采样

```go
rg := df.Resample("date", dataframe.ResampleMonthly) // D/W/M/Y/H
monthly := rg.Aggregation(
    []dataframe.AggregationType{dataframe.Aggregation_SUM},
    []string{"revenue"},
)
// 结果含 "period" 列 + 聚合列
```

#### 并行操作

```go
df.CapplyParallel(f)                                    // 并行按列 apply
df.RapplyParallel(f)                                    // 并行按行 apply
groups.AggregationParallel(typs, colnames)              // 并行 GroupBy 聚合
```

---

### 更多 I/O API（v1.2.1）

#### JSON Lines（NDJSON）

```go
// 读
df := dataframe.ReadNDJSON(r)

// 写（NaN → null）
err := df.WriteNDJSON(w)
```

#### Excel — 工作表选择

```go
df := dataframe.ReadXLSXFile("data.xlsx", dataframe.WithSheet("Sheet2"))
```

#### SQL — 占位符风格

```go
// PostgreSQL（$1, $2, …）
err := df.WriteSQL(pgDB, "users",
    dataframe.WithPlaceholderStyle(dataframe.SQLPlaceholderDollar))

// SQL Server（@p1, @p2, …）
err := df.WriteSQL(msDB, "users",
    dataframe.WithPlaceholderStyle(dataframe.SQLPlaceholderAt))
```

#### CSV 流式读取

```go
err := dataframe.ScanCSV(f, 1000, func(batch dataframe.DataFrame) error {
    // 处理 1000 行的批次
    return nil
}, dataframe.DetectDelimiter(true))
```

---

## 许可证

Apache-2.0 — 见 [LICENSE.md](LICENSE.md)
