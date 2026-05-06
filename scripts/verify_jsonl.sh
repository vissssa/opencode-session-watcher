#!/usr/bin/env bash
# verify_jsonl.sh — 验证本地 JSONL 文件与 open-code API 数据的一致性
#
# 功能：
#   1. 从 open-code API 获取所有 Session 列表
#   2. 对每个 Session 拉取完整消息列表（自动分页）
#   3. 按本地路径规则（{output_dir}/{user_id}/{agent_id}/{session_id}.jsonl）定位 JSONL 文件
#   4. 逐条校验 JSONL 文件中的 message_id 与 API 返回顺序是否完全一致
#   5. 汇总输出校验结果，发现不一致时以非零状态码退出
#
# 用法：
#   ./scripts/verify_jsonl.sh [选项]
#
# 选项：
#   -u URL          open-code 服务地址（默认 http://localhost:57811）
#   -d DIR          JSONL 输出根目录（默认 ./data/messages）
#   -l LIMIT        单次拉取消息数量上限（默认 1000）
#   -s SESSION_ID   只校验指定 Session（默认校验全部）
#   -v              详细输出，打印每条消息的校验过程
#   -h              显示帮助

set -euo pipefail

# ─── 默认参数 ──────────────────────────────────────────────────────────────────
BASE_URL="http://localhost:57811"
OUTPUT_DIR="./data/messages"
MSG_LIMIT=1000
FILTER_SESSION=""
VERBOSE=false

# ─── 颜色输出 ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET} $*"; }
ok()      { echo -e "${GREEN}[PASS]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
fail()    { echo -e "${RED}[FAIL]${RESET} $*"; }
verbose() { $VERBOSE && echo -e "       $*" || true; }

# ─── 依赖检查 ──────────────────────────────────────────────────────────────────
check_deps() {
    local missing=()
    for cmd in curl jq; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        fail "缺少依赖命令：${missing[*]}"
        echo "  macOS: brew install ${missing[*]}"
        echo "  Linux: apt-get install ${missing[*]} / yum install ${missing[*]}"
        exit 1
    fi
}

# ─── 参数解析 ──────────────────────────────────────────────────────────────────
usage() {
    sed -n '/^# 用法/,/^$/p' "$0" | grep -v '^#' || true
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -u URL          open-code 服务地址（默认 $BASE_URL）"
    echo "  -d DIR          JSONL 输出根目录（默认 $OUTPUT_DIR）"
    echo "  -l LIMIT        单次拉取消息数量上限（默认 $MSG_LIMIT）"
    echo "  -s SESSION_ID   只校验指定 Session"
    echo "  -v              详细输出"
    echo "  -h              显示帮助"
    exit 0
}

while getopts "u:d:l:s:vh" opt; do
    case $opt in
        u) BASE_URL="$OPTARG" ;;
        d) OUTPUT_DIR="$OPTARG" ;;
        l) MSG_LIMIT="$OPTARG" ;;
        s) FILTER_SESSION="$OPTARG" ;;
        v) VERBOSE=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

# ─── 路径清洗函数（与 Go 实现完全一致）──────────────────────────────────────────
# cleanSegment 仅保留字母、数字、'-'、'_'、'.'，其余替换为 '_'，
# 并去掉首尾的点；空串/"."/".."/清洗后为空 → 返回 "unknown"
clean_segment() {
    local val="$1"
    # 去首尾空白
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"

    if [[ -z "$val" || "$val" == "." || "$val" == ".." ]]; then
        echo "unknown"
        return
    fi

    # 字符白名单过滤（仅 ASCII 子集，与 Go unicode.IsLetter/IsDigit + -_.）
    local cleaned
    cleaned=$(echo "$val" | LC_ALL=C sed 's/[^a-zA-Z0-9._-]/_/g')

    # 去掉首尾的点
    cleaned="${cleaned#.}"
    cleaned="${cleaned%.}"
    # 去掉连续的首尾点（如 "..foo.."）
    cleaned=$(echo "$cleaned" | LC_ALL=C sed 's/^\.*//;s/\.*$//')

    if [[ -z "$cleaned" || "$cleaned" == ".." ]]; then
        echo "unknown"
        return
    fi
    echo "$cleaned"
}

# jsonl_path 根据 user_id / agent_id / session_id 计算本地文件路径
jsonl_path() {
    local user_id agent_id session_id
    user_id=$(clean_segment "$1")
    agent_id=$(clean_segment "$2")
    session_id=$(clean_segment "$3")
    echo "${OUTPUT_DIR}/${user_id}/${agent_id}/${session_id}.jsonl"
}

# ─── API 请求函数 ──────────────────────────────────────────────────────────────

# api_get 发送 GET 请求并返回响应体；非 2xx 或连接失败时打印错误并退出
api_get() {
    local path="$1"
    local url="${BASE_URL}${path}"

    # 用临时文件存响应体，-w 将状态码输出到 stdout，两者通过文件隔离
    local tmp_body
    tmp_body=$(mktemp)

    local http_code
    # -s 静默, -S 仍显示 curl 错误, -o 写响应体到临时文件, -w 输出状态码到 stdout
    if ! http_code=$(curl -sS --max-time 30 \
                         -o "$tmp_body" \
                         -w "%{http_code}" \
                         "$url" 2>&1); then
        # curl 自身失败（连接拒绝、超时、DNS 失败等），$http_code 含错误信息
        rm -f "$tmp_body"
        fail "请求失败: GET $url"
        fail "$http_code"
        exit 1
    fi

    local body
    body=$(cat "$tmp_body")
    rm -f "$tmp_body"

    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        fail "HTTP $http_code: GET $url"
        fail "响应: ${body:0:200}"
        exit 1
    fi
    echo "$body"
}

# list_sessions 返回所有 Session 的 JSON 数组
list_sessions() {
    api_get "/session"
}

# list_messages 返回指定 Session 的消息 JSON 数组（拉取 limit 条）
list_messages() {
    local session_id="$1"
    local limit="$2"
    # URL 编码 session_id（处理含斜杠等特殊字符的 ID）
    local encoded
    encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1],safe=''))" "$session_id" 2>/dev/null \
        || python -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1],safe=''))" "$session_id" 2>/dev/null \
        || echo "$session_id")
    api_get "/session/${encoded}/message?limit=${limit}"
}

# ─── 单 Session 校验 ──────────────────────────────────────────────────────────

# verify_session 校验单个 Session 的 JSONL 与 API 消息一致性
# 返回值：0=通过, 1=失败, 2=跳过（文件不存在）
verify_session() {
    local session_json="$1"

    # 解析 Session 元数据
    local session_id user_id agent_id
    session_id=$(echo "$session_json" | jq -r '.id // empty')
    if [[ -z "$session_id" ]]; then
        warn "Session JSON 缺少 id 字段，跳过"
        return 2
    fi

    # 兼容 snake_case 和 camelCase 字段，与 Go 的 firstNonEmpty 逻辑一致
    user_id=$(echo "$session_json" | jq -r '
        if (.user_id // "" | length) > 0 then .user_id
        elif (.userID // "" | length) > 0 then .userID
        else "default_user"
        end
    ')
    agent_id=$(echo "$session_json" | jq -r '
        if (.agent_id // "" | length) > 0 then .agent_id
        elif (.agentID // "" | length) > 0 then .agentID
        else "default_agent"
        end
    ')

    local jsonl_file
    jsonl_file=$(jsonl_path "$user_id" "$agent_id" "$session_id")

    verbose "Session: $session_id  user=$user_id  agent=$agent_id"
    verbose "JSONL 路径: $jsonl_file"

    # ── 步骤 1：检查 JSONL 文件是否存在 ────────────────────────────────────────
    if [[ ! -f "$jsonl_file" ]]; then
        # 拉取消息数量，若为 0 则不需要文件，跳过
        local msg_count
        msg_count=$(list_messages "$session_id" "1" | jq 'length')
        if [[ "$msg_count" -eq 0 ]]; then
            verbose "API 返回 0 条消息，无需 JSONL 文件，跳过"
            return 2
        fi
        fail "Session $session_id: JSONL 文件不存在（${jsonl_file}），但 API 有消息"
        return 1
    fi

    # ── 步骤 2：从 API 获取完整消息列表 ─────────────────────────────────────────
    local api_msgs_json
    api_msgs_json=$(list_messages "$session_id" "$MSG_LIMIT")

    local api_count
    api_count=$(echo "$api_msgs_json" | jq 'length')
    verbose "API 返回消息数: $api_count"

    if [[ "$api_count" -eq 0 ]]; then
        warn "Session $session_id: API 返回 0 条消息，但 JSONL 文件存在（可能为历史数据），跳过"
        return 2
    fi

    if [[ "$api_count" -ge "$MSG_LIMIT" ]]; then
        warn "Session $session_id: API 返回消息数 ($api_count) 已达 limit ($MSG_LIMIT)，" \
             "可能存在截断，建议增大 -l 参数"
    fi

    # 从 API JSON 中提取有序的 message_id 列表
    # API 消息结构: { info: { id, sessionID, time: { created } }, ... }
    local api_ids
    api_ids=$(echo "$api_msgs_json" | jq -r '.[].info.id')

    # ── 步骤 3：从 JSONL 文件中提取 message_id 列表 ─────────────────────────────
    local jsonl_count
    jsonl_count=$(wc -l < "$jsonl_file" | tr -d ' ')
    verbose "JSONL 行数: $jsonl_count"

    # JSONL 每行是一个 MessageRecord，包含 message_id 字段
    local jsonl_ids
    jsonl_ids=$(jq -r '.message_id' "$jsonl_file" 2>/dev/null || {
        fail "Session $session_id: JSONL 文件解析失败（${jsonl_file}）"
        return 1
    })

    # ── 步骤 4：校验 API 中的每条消息都存在于 JSONL 且顺序一致 ─────────────────────
    #
    # 规则：JSONL 必须包含 API 返回的所有消息（超集），
    # 且 API 消息在 JSONL 中出现的顺序与 API 返回顺序相同（子序列关系）。
    # 兼容两种正常情况：
    #   - JSONL 消息多于 API：JSONL 可包含 API 未返回的额外历史消息，不视为错误
    #   - JSONL 存在重复消息：at-least-once 语义下同一消息可能被写多次，
    #     取第一次出现的行号参与顺序判断，后续重复行忽略
    #
    # 实现用 awk 代替 bash 关联数组（bash 4+ 特性），兼容 macOS 自带 bash 3.2。
    # awk 程序接收两段输入：
    #   1) JSONL 的 message_id 列表（每行一个 id），用于建立 id→首次行号 映射
    #   2) 分隔符 "---SPLIT---"
    #   3) API 的 message_id 列表（每行一个 id），逐条查表验证
    # awk 输出格式：
    #   DUP <count>          — JSONL 重复条数
    #   OK <id> <lineno>     — 校验通过
    #   MISSING <id>         — JSONL 中缺失
    #   ORDER <id> <cur> <prev> — 顺序错误

    local awk_result
    awk_result=$(awk '
        BEGIN { phase=1; dup=0 }
        /^---SPLIT---$/ { phase=2; next }
        phase==1 {
            id = $0
            if (id == "") next
            if (id in line_map) {
                dup++
            } else {
                line_map[id] = NR   # 首次出现行号
            }
            next
        }
        phase==2 {
            id = $0
            if (id == "") next
            if (!(id in line_map)) {
                print "MISSING", id
                next
            }
            cur = line_map[id]
            if (cur <= prev_line) {
                print "ORDER", id, cur, prev_line
            } else {
                print "OK", id, cur
            }
            prev_line = cur
        }
        END { print "DUP", dup }
    ' <<< "${jsonl_ids}
---SPLIT---
${api_ids}")

    # 解析 awk 输出
    local errors=0
    local checked=0
    local dup_count=0

    while IFS= read -r line; do
        local kind
        kind=$(echo "$line" | awk '{print $1}')
        case "$kind" in
            DUP)
                dup_count=$(echo "$line" | awk '{print $2}')
                ;;
            OK)
                ((checked++)) || true
                local ok_id ok_lineno
                ok_id=$(echo "$line" | awk '{print $2}')
                ok_lineno=$(echo "$line" | awk '{print $3}')
                verbose "  ✓ $ok_id (JSONL line $ok_lineno)"
                ;;
            MISSING)
                ((checked++)) || true
                local miss_id
                miss_id=$(echo "$line" | awk '{print $2}')
                fail "Session $session_id: 消息 $miss_id 在 API 中存在，但 JSONL 中缺失"
                ((errors++)) || true
                ;;
            ORDER)
                ((checked++)) || true
                local ord_id ord_cur ord_prev
                ord_id=$(echo "$line" | awk '{print $2}')
                ord_cur=$(echo "$line" | awk '{print $3}')
                ord_prev=$(echo "$line" | awk '{print $4}')
                fail "Session $session_id: 消息顺序错误 — $ord_id 在 JSONL 第 $ord_cur 行，" \
                     "但前一条消息在第 $ord_prev 行（期望递增）"
                ((errors++)) || true
                ;;
        esac
    done <<< "$awk_result"

    if [[ "$dup_count" -gt 0 ]]; then
        warn "Session $session_id: JSONL 中存在 $dup_count 条重复消息（at-least-once 写入），已忽略重复行"
    fi

    if [[ "$errors" -gt 0 ]]; then
        fail "Session $session_id: 发现 $errors 个问题（API $api_count 条，JSONL $jsonl_count 行，校验 $checked 条）"
        return 1
    fi

    ok "Session $session_id: 全部 $checked 条消息校验通过（JSONL $jsonl_count 行）"
    return 0
}

# ─── 主流程 ────────────────────────────────────────────────────────────────────
main() {
    check_deps

    echo ""
    echo -e "${BOLD}====== session_watcher JSONL 一致性校验 ======${RESET}"
    echo "  open-code 服务: $BASE_URL"
    echo "  JSONL 根目录:   $OUTPUT_DIR"
    echo "  消息拉取上限:   $MSG_LIMIT"
    [[ -n "$FILTER_SESSION" ]] && echo "  过滤 Session:   $FILTER_SESSION"
    echo ""

    # ── 1. 获取 Session 列表 ──────────────────────────────────────────────────
    info "正在获取 Session 列表..."
    local sessions_json
    sessions_json=$(list_sessions)

    local total_sessions
    total_sessions=$(echo "$sessions_json" | jq 'length')
    info "共发现 $total_sessions 个 Session"

    if [[ "$total_sessions" -eq 0 ]]; then
        warn "API 返回 0 个 Session，无需校验"
        exit 0
    fi

    # ── 2. 逐 Session 校验 ────────────────────────────────────────────────────
    local pass=0 fail_count=0 skip=0

    while IFS= read -r session_json; do
        local sid
        sid=$(echo "$session_json" | jq -r '.id // empty')

        # 如果指定了 -s，只校验该 Session
        if [[ -n "$FILTER_SESSION" && "$sid" != "$FILTER_SESSION" ]]; then
            continue
        fi

        local result
        verify_session "$session_json"
        result=$?

        case $result in
            0) ((pass++)) || true ;;
            1) ((fail_count++)) || true ;;
            2) ((skip++)) || true ;;
        esac
    done < <(echo "$sessions_json" | jq -c '.[]')

    # ── 3. 汇总输出 ────────────────────────────────────────────────────────────
    echo ""
    echo -e "${BOLD}====== 校验结果汇总 ======${RESET}"
    echo -e "  ${GREEN}通过: $pass${RESET}"
    echo -e "  ${RED}失败: $fail_count${RESET}"
    echo -e "  ${YELLOW}跳过: $skip${RESET}"
    echo ""

    if [[ "$fail_count" -gt 0 ]]; then
        fail "存在 $fail_count 个 Session 校验失败，请检查上方输出"
        exit 1
    fi

    ok "所有校验通过"
    exit 0
}

main "$@"
