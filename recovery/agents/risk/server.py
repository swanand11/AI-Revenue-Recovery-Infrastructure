from recovery.agents.debug_runner import run_agent_debug


def main() -> None:
    run_agent_debug("risk-agent", "risk-v1", "risk-and-duplicate-recovery-guardrail")


if __name__ == "__main__":
    main()
