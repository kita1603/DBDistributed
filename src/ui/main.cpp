// Minimal Dear ImGui desktop client for a single shard's Raft cluster.
//
// Deliberately Windows-only (Win32 + DirectX11 backend) - see README.md's
// "raftui" section for why this trade-off was chosen (no separate SDK to
// install, unlike Qt/wxWidgets). v1 scope: enter one shard's peer list,
// send one SQL/REPL statement at a time, see the result - a graphical
// single-shard REPL. It reuses the exact same client-side logic
// raftnode.exe's own REPL uses (src/raft/client.h's SendClientRequest/
// SendReadRequest, plus the SQL parser to decide read vs. write) rather
// than reimplementing any wire-protocol or dispatch logic. CREATE TABLE
// broadcast, cross-shard scatter/gather, and 2PC transactions are all
// orchestration logic that only exists in raft_main.cpp's REPL today, not
// as reusable library calls - out of scope here, a larger follow-up if
// ever wanted.

#include <d3d11.h>
#include <windows.h>

#include <chrono>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "imgui.h"
#include "imgui_impl_dx11.h"
#include "imgui_impl_win32.h"

#include "raft/client.h"
#include "raft/types.h"
#include "sql/ast.h"
#include "sql/lexer.h"
#include "sql/parser.h"

// Forward declare the Win32 message handler ImGui's backend provides -
// its own header intentionally doesn't include <windows.h> itself, so
// this project's .cpp is expected to copy this declaration (per
// imgui_impl_win32.h's own comment).
extern IMGUI_IMPL_API LRESULT ImGui_ImplWin32_WndProcHandler(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);

namespace {

// --- DirectX11 device/swapchain plumbing (standard ImGui example pattern) ---

ID3D11Device* g_pd3dDevice = nullptr;
ID3D11DeviceContext* g_pd3dDeviceContext = nullptr;
IDXGISwapChain* g_pSwapChain = nullptr;
bool g_SwapChainOccluded = false;
ID3D11RenderTargetView* g_mainRenderTargetView = nullptr;

void CreateRenderTarget() {
    ID3D11Texture2D* pBackBuffer = nullptr;
    g_pSwapChain->GetBuffer(0, IID_PPV_ARGS(&pBackBuffer));
    g_pd3dDevice->CreateRenderTargetView(pBackBuffer, nullptr, &g_mainRenderTargetView);
    pBackBuffer->Release();
}

void CleanupRenderTarget() {
    if (g_mainRenderTargetView) {
        g_mainRenderTargetView->Release();
        g_mainRenderTargetView = nullptr;
    }
}

bool CreateDeviceD3D(HWND hWnd) {
    DXGI_SWAP_CHAIN_DESC sd{};
    sd.BufferCount = 2;
    sd.BufferDesc.Width = 0;
    sd.BufferDesc.Height = 0;
    sd.BufferDesc.Format = DXGI_FORMAT_R8G8B8A8_UNORM;
    sd.BufferDesc.RefreshRate.Numerator = 60;
    sd.BufferDesc.RefreshRate.Denominator = 1;
    sd.Flags = DXGI_SWAP_CHAIN_FLAG_ALLOW_MODE_SWITCH;
    sd.BufferUsage = DXGI_USAGE_RENDER_TARGET_OUTPUT;
    sd.OutputWindow = hWnd;
    sd.SampleDesc.Count = 1;
    sd.SampleDesc.Quality = 0;
    sd.Windowed = TRUE;
    sd.SwapEffect = DXGI_SWAP_EFFECT_DISCARD;

    UINT createDeviceFlags = 0;
    D3D_FEATURE_LEVEL featureLevel;
    const D3D_FEATURE_LEVEL featureLevelArray[2] = {D3D_FEATURE_LEVEL_11_0, D3D_FEATURE_LEVEL_10_0};
    HRESULT res = D3D11CreateDeviceAndSwapChain(nullptr, D3D_DRIVER_TYPE_HARDWARE, nullptr, createDeviceFlags,
                                                 featureLevelArray, 2, D3D11_SDK_VERSION, &sd, &g_pSwapChain,
                                                 &g_pd3dDevice, &featureLevel, &g_pd3dDeviceContext);
    if (res == DXGI_ERROR_UNSUPPORTED) {
        res = D3D11CreateDeviceAndSwapChain(nullptr, D3D_DRIVER_TYPE_WARP, nullptr, createDeviceFlags,
                                             featureLevelArray, 2, D3D11_SDK_VERSION, &sd, &g_pSwapChain,
                                             &g_pd3dDevice, &featureLevel, &g_pd3dDeviceContext);
    }
    if (res != S_OK) return false;

    CreateRenderTarget();
    return true;
}

void CleanupDeviceD3D() {
    CleanupRenderTarget();
    if (g_pSwapChain) {
        g_pSwapChain->Release();
        g_pSwapChain = nullptr;
    }
    if (g_pd3dDeviceContext) {
        g_pd3dDeviceContext->Release();
        g_pd3dDeviceContext = nullptr;
    }
    if (g_pd3dDevice) {
        g_pd3dDevice->Release();
        g_pd3dDevice = nullptr;
    }
}

LRESULT CALLBACK WndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam) {
    if (ImGui_ImplWin32_WndProcHandler(hWnd, msg, wParam, lParam)) return true;

    switch (msg) {
        case WM_SIZE:
            if (g_pd3dDevice != nullptr && wParam != SIZE_MINIMIZED) {
                CleanupRenderTarget();
                g_pSwapChain->ResizeBuffers(0, (UINT)LOWORD(lParam), (UINT)HIWORD(lParam), DXGI_FORMAT_UNKNOWN, 0);
                CreateRenderTarget();
            }
            return 0;
        case WM_SYSCOMMAND:
            if ((wParam & 0xfff0) == SC_KEYMENU) return 0;  // no ALT application menu
            break;
        case WM_DESTROY:
            PostQuitMessage(0);
            return 0;
    }
    return DefWindowProc(hWnd, msg, wParam, lParam);
}

// --- Small local copy of the "id=host:port,id=host:port" peer-spec parser -
// same convention already used by routing.conf/membership_state.cpp, and
// the same "small private copy rather than a shared dependency" precedent
// membership_state.cpp's own comment explains: this is this file's own
// input-parsing detail, not a shared library concern. ---

std::map<distdb::NodeId, distdb::PeerAddress> ParsePeerSpec(const std::string& spec) {
    std::map<distdb::NodeId, distdb::PeerAddress> peers;
    std::istringstream stream(spec);
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        if (entry.empty()) continue;
        size_t eq = entry.find('=');
        size_t colon = entry.find(':', eq == std::string::npos ? 0 : eq);
        if (eq == std::string::npos || colon == std::string::npos) continue;  // skip a malformed entry
        try {
            auto id = static_cast<distdb::NodeId>(std::stoul(entry.substr(0, eq)));
            std::string host = entry.substr(eq + 1, colon - eq - 1);
            auto port = static_cast<uint16_t>(std::stoul(entry.substr(colon + 1)));
            peers[id] = {host, port};
        } catch (...) {
            // Malformed number in this entry - skip it, don't crash the UI over a typo.
        }
    }
    return peers;
}

// The inverse of ParsePeerSpec - used to auto-fill the Peers field after a
// successful Discover.
std::string FormatPeerSpec(const std::map<distdb::NodeId, distdb::PeerAddress>& peers) {
    std::string out;
    bool first = true;
    for (const auto& [id, addr] : peers) {
        if (!first) out += ',';
        first = false;
        out += std::to_string(id) + "=" + addr.host + ":" + std::to_string(addr.port);
    }
    return out;
}

// A bare "host:port" (no id= prefix, unlike ParsePeerSpec) - what the
// "Seed node" field takes, since DiscoverCluster only needs an address to
// connect to, not an id (it learns the id from the response itself).
bool ParseHostPort(const std::string& s, std::string& host, uint16_t& port) {
    size_t colon = s.find(':');
    if (colon == std::string::npos) return false;
    try {
        host = s.substr(0, colon);
        port = static_cast<uint16_t>(std::stoul(s.substr(colon + 1)));
        return !host.empty();
    } catch (...) {
        return false;
    }
}

// Adapted from raft_main.cpp's PrintClientResponse - same formatting,
// returned as a string instead of printed to stdout.
std::string FormatClientResponse(const distdb::ClientResponse& resp) {
    if (resp.success) {
        return "OK (committed at index " + std::to_string(resp.index) + ", via leader node " +
               std::to_string(resp.leader_hint) + ")";
    }
    std::string out = "FAILED";
    if (resp.not_leader && resp.leader_hint != 0) {
        out += " (best known leader is node " + std::to_string(resp.leader_hint) + ", but still failed";
        if (!resp.error.empty()) out += ": " + resp.error;
        out += " - retry)";
    } else if (!resp.error.empty()) {
        out += " (" + resp.error + ")";
    }
    return out;
}

enum class ResultKind { kText, kTable, kError };

struct StatementResult {
    ResultKind kind = ResultKind::kText;
    std::string text;
};

// Sends one statement to the cluster: parses it to decide read vs. write
// (mirroring raft_main.cpp's own dispatch), then calls the same client
// library functions the REPL itself uses. Blocking - see main.cpp's top
// comment/README for why that's an accepted v1 simplification. The
// resulting `kind` tells the render loop whether `text` is grid-worthy
// (SqlExecutor::ExecuteSelect's tab-separated header+rows format) rather
// than having it re-sniff the statement/response itself.
StatementResult SendStatement(const std::map<distdb::NodeId, distdb::PeerAddress>& peers, const std::string& line) {
    if (peers.empty()) return {ResultKind::kError, "ERROR: no peers configured - enter a peer list first"};

    // SHOW TABLES is a read exactly like SELECT here too: raftui never
    // scatters across shards (out of scope, see this file's top comment),
    // so it just needs *a* reachable peer to answer, same as SELECT -
    // SendReadRequest already tries every peer in turn regardless of
    // which shard it belongs to.
    bool is_read = false;
    try {
        distdb::Parser parser(distdb::Tokenize(line));
        distdb::Statement parsed = parser.ParseStatement();
        is_read = std::holds_alternative<distdb::SelectStatement>(parsed) ||
                  std::holds_alternative<distdb::ShowTablesStatement>(parsed);
    } catch (const std::exception& e) {
        return {ResultKind::kError, std::string("ERROR: ") + e.what()};
    }

    constexpr int kTimeoutMs = 3000;
    if (is_read) {
        try {
            return {ResultKind::kTable, distdb::SendReadRequest(peers, line, kTimeoutMs)};
        } catch (const std::exception& e) {
            return {ResultKind::kError, std::string("ERROR: ") + e.what()};
        }
    }
    // SendClientRequest already catches network-level failures internally
    // (an unreachable peer), but DecodeClientResponse - called *after*
    // that, still inside SendClientRequest - can itself throw
    // std::runtime_error on a malformed/truncated response, and that
    // exception isn't caught anywhere inside client.cpp. Left uncaught
    // here too, it would propagate all the way out of this function with
    // nothing above it in main()'s loop to catch it either - an
    // exception escaping past that point is an unhandled exception at
    // the top of the call stack, which terminates the whole process
    // instead of just failing this one send.
    try {
        distdb::ClientResponse resp = distdb::SendClientRequest(peers, line, kTimeoutMs);
        return {resp.success ? ResultKind::kText : ResultKind::kError, FormatClientResponse(resp)};
    } catch (const std::exception& e) {
        return {ResultKind::kError, std::string("ERROR: ") + e.what()};
    }
}

// Parses SqlExecutor::ExecuteSelect's own output format (a tab-separated
// header line, then one tab-separated line per matching row, or a literal
// "(0 rows)" line when none matched) into a grid the render loop can hand
// to ImGui's table API. Returns false (leaving `headers`/`rows` untouched)
// if `text` doesn't even have a header line - the render loop falls back
// to plain text in that case rather than showing an empty table.
bool TryParseSelectTable(const std::string& text, std::vector<std::string>& headers,
                          std::vector<std::vector<std::string>>& rows) {
    std::istringstream stream(text);
    std::string header_line;
    if (!std::getline(stream, header_line) || header_line.empty()) return false;

    std::vector<std::string> parsed_headers;
    std::istringstream header_stream(header_line);
    std::string col;
    while (std::getline(header_stream, col, '\t')) parsed_headers.push_back(col);
    if (parsed_headers.empty()) return false;

    std::vector<std::vector<std::string>> parsed_rows;
    std::string line;
    while (std::getline(stream, line)) {
        if (line == "(0 rows)") continue;  // trailing marker, not a data row
        std::vector<std::string> row;
        std::istringstream line_stream(line);
        std::string val;
        while (std::getline(line_stream, val, '\t')) row.push_back(val);
        parsed_rows.push_back(std::move(row));
    }

    headers = std::move(parsed_headers);
    rows = std::move(parsed_rows);
    return true;
}

// One entry in the output history panel - either a plain line (command
// echo, OK/FAILED/ERROR text) or a parsed SELECT result rendered as a
// bordered grid (see TryParseSelectTable).
struct HistoryEntry {
    ResultKind kind = ResultKind::kText;
    std::string text;
    std::vector<std::string> headers;
    std::vector<std::vector<std::string>> rows;
    // Only set for an actual SendStatement result (not the "> <command>"
    // echo, and not the Discover button's own messages) - see the render
    // loop, which only prints an elapsed-time line when this is true.
    bool has_timing = false;
    double elapsed_ms = 0.0;
};

HistoryEntry MakeHistoryEntry(ResultKind kind, std::string text) {
    HistoryEntry entry;
    bool parsed_as_table = kind == ResultKind::kTable && TryParseSelectTable(text, entry.headers, entry.rows);
    entry.kind = parsed_as_table ? ResultKind::kTable : (kind == ResultKind::kTable ? ResultKind::kText : kind);
    entry.text = std::move(text);
    return entry;
}

}  // namespace

int WINAPI WinMain(HINSTANCE hInstance, HINSTANCE, LPSTR, int) {
    WNDCLASSEXW wc = {sizeof(wc),        CS_CLASSDC, WndProc, 0L,   0L, hInstance, nullptr, nullptr,
                       nullptr, nullptr, L"raftui",   nullptr};
    ::RegisterClassExW(&wc);
    HWND hwnd = ::CreateWindowW(wc.lpszClassName, L"raftui - Raft cluster client", WS_OVERLAPPEDWINDOW, 100, 100,
                                 900, 600, nullptr, nullptr, wc.hInstance, nullptr);

    if (!CreateDeviceD3D(hwnd)) {
        CleanupDeviceD3D();
        ::UnregisterClassW(wc.lpszClassName, wc.hInstance);
        return 1;
    }

    ::ShowWindow(hwnd, SW_SHOWDEFAULT);
    ::UpdateWindow(hwnd);

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();

    ImGui_ImplWin32_Init(hwnd);
    ImGui_ImplDX11_Init(g_pd3dDevice, g_pd3dDeviceContext);

    std::map<distdb::NodeId, distdb::PeerAddress> peers;
    char seed_buf[128] = "127.0.0.1:9001";
    char peers_buf[256] = "1=127.0.0.1:9001,2=127.0.0.1:9002";
    char command_buf[512] = "";
    std::vector<HistoryEntry> history;
    bool scroll_to_bottom = false;

    bool done = false;
    while (!done) {
        MSG msg;
        while (::PeekMessage(&msg, nullptr, 0U, 0U, PM_REMOVE)) {
            ::TranslateMessage(&msg);
            ::DispatchMessage(&msg);
            if (msg.message == WM_QUIT) done = true;
        }
        if (done) break;

        if (g_SwapChainOccluded && g_pSwapChain->Present(0, DXGI_PRESENT_TEST) == DXGI_STATUS_OCCLUDED) {
            ::Sleep(10);
            continue;
        }
        g_SwapChainOccluded = false;

        ImGui_ImplDX11_NewFrame();
        ImGui_ImplWin32_NewFrame();
        ImGui::NewFrame();

        ImGui::SetNextWindowPos(ImVec2(0, 0));
        ImGui::SetNextWindowSize(io.DisplaySize);
        ImGui::Begin("raftui", nullptr,
                      ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove |
                          ImGuiWindowFlags_NoCollapse);

        ImGui::TextUnformatted("Seed node (host:port) - only need one, the rest are discovered:");
        ImGui::SetNextItemWidth(-100);
        ImGui::InputText("##seed", seed_buf, sizeof(seed_buf));
        ImGui::SameLine();
        if (ImGui::Button("Discover")) {
            std::string host;
            uint16_t port = 0;
            if (!ParseHostPort(seed_buf, host, port)) {
                history.push_back(MakeHistoryEntry(ResultKind::kError, "ERROR: seed must be host:port"));
            } else {
                try {
                    auto discovered = distdb::DiscoverCluster(host, port, 3000);
                    std::string formatted = FormatPeerSpec(discovered);
                    size_t n = formatted.copy(peers_buf, sizeof(peers_buf) - 1);
                    peers_buf[n] = '\0';
                    history.push_back(MakeHistoryEntry(ResultKind::kText,
                                                        "Discovered " + std::to_string(discovered.size()) +
                                                            " peer(s) from " + std::string(seed_buf) + ": " +
                                                            formatted));
                } catch (const std::exception& e) {
                    history.push_back(MakeHistoryEntry(ResultKind::kError, std::string("ERROR: ") + e.what()));
                }
                scroll_to_bottom = true;
            }
        }

        ImGui::TextUnformatted("Peers (id=host:port,id=host:port,...) - auto-filled by Discover, editable:");
        ImGui::SetNextItemWidth(-1);
        ImGui::InputText("##peers", peers_buf, sizeof(peers_buf));
        peers = ParsePeerSpec(peers_buf);
        ImGui::SameLine();
        ImGui::Text("(%zu peer(s) parsed)", peers.size());

        ImGui::Separator();

        ImGui::TextUnformatted("Output:");
        ImGui::BeginChild("output", ImVec2(0, -60), ImGuiChildFlags_Borders);
        for (size_t i = 0; i < history.size(); i++) {
            const HistoryEntry& entry = history[i];
            if (entry.kind == ResultKind::kTable) {
                std::string table_id = "##table" + std::to_string(i);
                ImGuiTableFlags flags = ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                                        ImGuiTableFlags_Resizable | ImGuiTableFlags_SizingStretchProp;
                if (ImGui::BeginTable(table_id.c_str(), static_cast<int>(entry.headers.size()), flags)) {
                    for (const auto& header : entry.headers) ImGui::TableSetupColumn(header.c_str());
                    ImGui::TableHeadersRow();
                    for (const auto& row : entry.rows) {
                        ImGui::TableNextRow();
                        for (const auto& cell : row) {
                            ImGui::TableNextColumn();
                            ImGui::TextUnformatted(cell.c_str());
                        }
                    }
                    ImGui::EndTable();
                }
                if (entry.rows.empty()) ImGui::TextDisabled("(0 rows)");
            } else if (entry.kind == ResultKind::kError) {
                ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.45f, 0.45f, 1.0f));
                ImGui::TextWrapped("%s", entry.text.c_str());
                ImGui::PopStyleColor();
            } else {
                ImGui::TextWrapped("%s", entry.text.c_str());
            }
            if (entry.has_timing) ImGui::TextDisabled("(%.2fms)", entry.elapsed_ms);
        }
        if (scroll_to_bottom) {
            ImGui::SetScrollHereY(1.0f);
            scroll_to_bottom = false;
        }
        ImGui::EndChild();

        // Shared by the command box's Send and the Tables shortcut button
        // below - both just run a statement and append the same "> ..."
        // echo plus timed result to history.
        auto run_statement = [&](const std::string& line) {
            history.push_back(MakeHistoryEntry(ResultKind::kText, "> " + line));

            auto stmt_start = std::chrono::steady_clock::now();
            StatementResult result = SendStatement(peers, line);
            double elapsed_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() -
                                                                            stmt_start)
                                     .count();

            HistoryEntry entry = MakeHistoryEntry(result.kind, std::move(result.text));
            entry.has_timing = true;
            entry.elapsed_ms = elapsed_ms;
            history.push_back(std::move(entry));

            scroll_to_bottom = true;
        };

        ImGui::SetNextItemWidth(-160);
        bool send = ImGui::InputText("##command", command_buf, sizeof(command_buf),
                                      ImGuiInputTextFlags_EnterReturnsTrue);
        ImGui::SameLine();
        send = ImGui::Button("Send") || send;
        ImGui::SameLine();
        if (ImGui::Button("Tables")) run_statement("SHOW TABLES");

        if (send && command_buf[0] != '\0') {
            run_statement(command_buf);
            command_buf[0] = '\0';
        }

        ImGui::End();

        ImGui::Render();
        const float clear_color[4] = {0.06f, 0.06f, 0.08f, 1.0f};
        g_pd3dDeviceContext->OMSetRenderTargets(1, &g_mainRenderTargetView, nullptr);
        g_pd3dDeviceContext->ClearRenderTargetView(g_mainRenderTargetView, clear_color);
        ImGui_ImplDX11_RenderDrawData(ImGui::GetDrawData());

        HRESULT hr = g_pSwapChain->Present(1, 0);
        g_SwapChainOccluded = (hr == DXGI_STATUS_OCCLUDED);
    }

    ImGui_ImplDX11_Shutdown();
    ImGui_ImplWin32_Shutdown();
    ImGui::DestroyContext();

    CleanupDeviceD3D();
    ::DestroyWindow(hwnd);
    ::UnregisterClassW(wc.lpszClassName, wc.hInstance);

    return 0;
}
