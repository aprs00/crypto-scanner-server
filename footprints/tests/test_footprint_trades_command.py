from unittest.mock import Mock, call, patch

from django.core.management.base import CommandError
from django.test import SimpleTestCase

from footprints.management.commands import footprint_trades


class FootprintTradesCommandTests(SimpleTestCase):
    @patch.object(footprint_trades.signal, "signal")
    @patch.object(footprint_trades, "FootprintCollector")
    def test_signals_stop_and_promote_the_standby_collector(
        self,
        collector_class,
        install_signal,
    ):
        sigusr1 = getattr(footprint_trades.signal, "SIGUSR1", None)
        if sigusr1 is None:
            self.skipTest("SIGUSR1 is not available on this platform")

        collector = collector_class.return_value
        previous_sigterm = Mock(name="previous_sigterm")
        previous_sigusr1 = Mock(name="previous_sigusr1")
        install_signal.side_effect = [
            previous_sigterm,
            previous_sigusr1,
            Mock(),
            Mock(),
        ]

        def run():
            installed = {
                installed_call.args[0]: installed_call.args[1]
                for installed_call in install_signal.call_args_list
            }
            installed[sigusr1](sigusr1, None)
            installed[footprint_trades.signal.SIGTERM](
                footprint_trades.signal.SIGTERM,
                None,
            )

        collector.run.side_effect = run

        footprint_trades.Command().handle(exchange="bybit", standby=True)

        collector_class.assert_called_once_with("bybit", standby=True)
        collector.promote.assert_called_once_with()
        collector.stop.assert_called_once_with()
        self.assertEqual(
            [member.args[0] for member in install_signal.call_args_list],
            [
                footprint_trades.signal.SIGTERM,
                sigusr1,
                sigusr1,
                footprint_trades.signal.SIGTERM,
            ],
        )
        self.assertEqual(install_signal.call_args_list[-2].args[1], previous_sigusr1)
        self.assertEqual(install_signal.call_args_list[-1].args[1], previous_sigterm)

    @patch.object(footprint_trades.signal, "signal")
    @patch.object(footprint_trades, "FootprintCollector")
    def test_signal_handlers_are_restored_when_the_collector_fails(
        self,
        collector_class,
        install_signal,
    ):
        sigusr1 = getattr(footprint_trades.signal, "SIGUSR1", None)
        if sigusr1 is None:
            self.skipTest("SIGUSR1 is not available on this platform")
        previous_sigterm = Mock(name="previous_sigterm")
        previous_sigusr1 = Mock(name="previous_sigusr1")
        install_signal.side_effect = [
            previous_sigterm,
            previous_sigusr1,
            Mock(),
            Mock(),
        ]
        collector_class.return_value.run.side_effect = RuntimeError("failed")

        with self.assertRaisesRegex(RuntimeError, "failed"):
            footprint_trades.Command().handle(exchange="binance", standby=True)

        self.assertEqual(
            install_signal.call_args_list[-2:],
            [
                call(sigusr1, previous_sigusr1),
                call(footprint_trades.signal.SIGTERM, previous_sigterm),
            ],
        )

    @patch.object(footprint_trades.signal, "signal")
    @patch.object(footprint_trades, "FootprintCollector")
    def test_sigterm_is_restored_when_sigusr1_registration_fails(
        self,
        collector_class,
        install_signal,
    ):
        sigusr1 = getattr(footprint_trades.signal, "SIGUSR1", None)
        if sigusr1 is None:
            self.skipTest("SIGUSR1 is not available on this platform")
        previous_sigterm = Mock(name="previous_sigterm")
        install_signal.side_effect = [
            previous_sigterm,
            ValueError("signals require the main thread"),
            Mock(),
        ]

        with self.assertRaisesRegex(ValueError, "main thread"):
            footprint_trades.Command().handle(exchange="binance", standby=True)

        collector_class.return_value.run.assert_not_called()
        self.assertEqual(
            install_signal.call_args_list[-1],
            call(footprint_trades.signal.SIGTERM, previous_sigterm),
        )

    @patch.object(footprint_trades, "FootprintCollector")
    def test_standby_requires_sigusr1(self, collector_class):
        with patch.object(footprint_trades.signal, "SIGUSR1", None, create=True):
            with self.assertRaisesRegex(CommandError, "requires SIGUSR1"):
                footprint_trades.Command().handle(
                    exchange="hyperliquid",
                    standby=True,
                )

        collector_class.assert_not_called()
