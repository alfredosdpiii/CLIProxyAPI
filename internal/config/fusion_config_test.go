package config

import "testing"

func TestParseConfigBytesFusionConfig(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`
fusion:
  enabled: true
  model: BryanFusion
  panel:
    - gpt-5.5
    - glm-5.2
    - kimi-k2.7
    - deepseek-v4-pro
  judge: gpt-5.5
  min-successes: 3
  simulated-streaming: true
`))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if !cfg.Fusion.Enabled {
		t.Fatal("Fusion.Enabled = false, want true")
	}
	if got := cfg.Fusion.Model; got != "BryanFusion" {
		t.Fatalf("Fusion.Model = %q, want BryanFusion", got)
	}
	if got := len(cfg.Fusion.Panel); got != 4 {
		t.Fatalf("len(Fusion.Panel) = %d, want 4", got)
	}
	if got := cfg.Fusion.Judge; got != "gpt-5.5" {
		t.Fatalf("Fusion.Judge = %q, want gpt-5.5", got)
	}
	if got := cfg.Fusion.MinSuccesses; got != 3 {
		t.Fatalf("Fusion.MinSuccesses = %d, want 3", got)
	}
	if !cfg.Fusion.SimulatedStreaming {
		t.Fatal("Fusion.SimulatedStreaming = false, want true")
	}
}
