"""
ChatRF Interlink - Peer Announce DTMF Module
=============================================
Press the configured DTMF key (default: 9) to hear
who is currently linked on the Interlink network.

Requires background_interlink.py to be loaded.
"""

from modules.base import DTMFModule


class InterlinkAnnounceModule(DTMFModule):
    name        = "Interlink Announce"
    version     = "1.0.0"
    description = "Announces linked interlink peers over the air"
    enabled     = True

    dtmf_command = '9'
    requires_rate_limit = True

    def handle_command(self):
        flag_name = self.repeater.module_manager._create_flag_name(self.name)
        self.repeater.module_manager.set_flag(flag_name)

    def execute(self):
        try:
            # Grab the running InterlinkModule instance from the module manager
            interlink = self._get_interlink_module()
            if interlink is None:
                self.repeater.speak_with_piper("Το Interlink δεν έχει φορτωθεί.")
                return
            interlink.announce_peers()
        except Exception as e:
            self.logger.error(f"InterlinkAnnounce error: {e}")

    def _get_interlink_module(self):
        """Find the InterlinkModule instance via the module manager."""
        for module in self.repeater.module_manager.modules:
            if type(module).__name__ == "InterlinkModule":
                return module
        return None
