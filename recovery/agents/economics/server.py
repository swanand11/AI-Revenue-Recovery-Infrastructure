from recovery.agents.debug_runner import run_agent_debug


def main() -> None:
    run_agent_debug("economics-agent", "economics-v1", "unit-economics-guardrail")


if __name__ == "__main__":
    main()
