from recovery.agents.debug_runner import run_agent_debug


def main() -> None:
    run_agent_debug("transaction-agent", "transaction-v1", "transaction-stage-recoverability")


if __name__ == "__main__":
    main()
