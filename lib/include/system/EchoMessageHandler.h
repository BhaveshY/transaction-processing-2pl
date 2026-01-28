#pragma once
#include "system/maelstrom.h"

namespace tp_project::system {
class EchoMessageHandler final : public maelstrom::MessageHandler {
public:
    [[nodiscard]] constexpr auto name() const -> std::string override;

    auto handle(const maelstrom::Message &request) -> void override;
};
}