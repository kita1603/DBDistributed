# Vendored Dear ImGui

Source: https://github.com/ocornut/imgui
Version: v1.91.5 (tag)
Commit: f401021d5a5d56fe2304056c391e78f81c8d4b8f (2024-11-07)
License: MIT (see imgui.cpp's header comment for the full license text)

Copied directly into this repo (no git submodule, no build-time fetch) -
only the core library and the Win32 + DirectX11 backend files needed by
`raftui` (`src/ui/main.cpp`). Not the full upstream repo (examples, docs,
other backends/bindings are not needed here).

Files:
- Core: `imgui.h`, `imgui.cpp`, `imgui_internal.h`, `imgui_draw.cpp`,
  `imgui_tables.cpp`, `imgui_widgets.cpp`, `imstb_rectpack.h`,
  `imstb_textedit.h`, `imstb_truetype.h`, `imconfig.h`.
- Backend: `backends/imgui_impl_win32.h/.cpp`, `backends/imgui_impl_dx11.h/.cpp`.

To upgrade: clone the new tag elsewhere, diff/copy the same file list over
these, update the version/commit above.
