(() => {
  const startBtn = document.getElementById("startBtn");
  const stopBtn = document.getElementById("stopBtn");
  const statusEl = document.getElementById("status");
  const logEl = document.getElementById("log");

  let ws = null;
  let audioContext = null;
  let mediaStream = null;
  let sourceNode = null;
  let processorNode = null;

  function appendLog(message) {
    const line = `[${new Date().toLocaleTimeString()}] ${message}`;
    logEl.textContent += `${line}\n`;
    logEl.scrollTop = logEl.scrollHeight;
  }

  function setStatus(text, flagged = null) {
    statusEl.textContent = text;
    statusEl.className = "pill";
    if (flagged === true) statusEl.classList.add("flagged");
    if (flagged === false) statusEl.classList.add("clean");
  }

  function downsampleBuffer(input, inSampleRate, outSampleRate) {
    if (outSampleRate === inSampleRate) return input;
    if (outSampleRate > inSampleRate) {
      throw new Error("Output sample rate must be lower than input sample rate");
    }

    const ratio = inSampleRate / outSampleRate;
    const outputLength = Math.round(input.length / ratio);
    const output = new Float32Array(outputLength);

    let inputIndex = 0;
    for (let i = 0; i < outputLength; i += 1) {
      const nextInputIndex = Math.round((i + 1) * ratio);
      let sum = 0;
      let count = 0;

      while (inputIndex < nextInputIndex && inputIndex < input.length) {
        sum += input[inputIndex];
        count += 1;
        inputIndex += 1;
      }

      output[i] = count > 0 ? sum / count : 0;
    }

    return output;
  }

  function floatTo16BitPCM(float32Buffer) {
    const int16Buffer = new Int16Array(float32Buffer.length);
    for (let i = 0; i < float32Buffer.length; i += 1) {
      const s = Math.max(-1, Math.min(1, float32Buffer[i]));
      int16Buffer[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
    }
    return int16Buffer;
  }

  async function startStreaming() {
    const scheme = window.location.protocol === "https:" ? "wss" : "ws";
    const wsUrl = `${scheme}://${window.location.host}/ws/flag-audio/`;

    ws = new WebSocket(wsUrl);
    ws.binaryType = "arraybuffer";

    ws.onopen = async () => {
      appendLog(`WebSocket connected: ${wsUrl}`);
      ws.send(JSON.stringify({ type: "start", sample_rate: 16000 }));
      setStatus("Listening", false);

      try {
        mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        sourceNode = audioContext.createMediaStreamSource(mediaStream);

        processorNode = audioContext.createScriptProcessor(4096, 1, 1);
        processorNode.onaudioprocess = (event) => {
          if (!ws || ws.readyState !== WebSocket.OPEN) return;

          const inputData = event.inputBuffer.getChannelData(0);
          const downsampled = downsampleBuffer(inputData, audioContext.sampleRate, 16000);
          const pcm16 = floatTo16BitPCM(downsampled);
          ws.send(pcm16.buffer);
        };

        sourceNode.connect(processorNode);
        processorNode.connect(audioContext.destination);
      } catch (error) {
        appendLog(`Audio capture error: ${error.message}`);
        setStatus("Error", true);
      }
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === "partial") {
          appendLog(`Partial: ${data.text}`);
        } else if (data.type === "segment") {
          appendLog(`Segment: ${data.text}`);
        } else if (data.type === "score") {
          appendLog(`Score: ${data.score} | flagged=${data.flagged}`);
          setStatus(data.flagged ? "Flagged" : "Listening", data.flagged);
        } else if (data.type === "score_error" || data.type === "error") {
          appendLog(`Error: ${data.error}`);
          setStatus("Error", true);
        } else if (data.type === "final") {
          appendLog(`Final transcript: ${data.transcript}`);
        } else {
          appendLog(`Event: ${event.data}`);
        }
      } catch (error) {
        appendLog(`Non-JSON message: ${event.data}`);
      }
    };

    ws.onerror = () => {
      appendLog("WebSocket error occurred.");
      setStatus("Error", true);
    };

    ws.onclose = () => {
      appendLog("WebSocket closed.");
      cleanupAudio();
      startBtn.disabled = false;
      stopBtn.disabled = true;
      if (statusEl.textContent !== "Error") {
        setStatus("Idle");
      }
    };

    startBtn.disabled = true;
    stopBtn.disabled = false;
  }

  function cleanupAudio() {
    if (processorNode) {
      processorNode.disconnect();
      processorNode.onaudioprocess = null;
      processorNode = null;
    }
    if (sourceNode) {
      sourceNode.disconnect();
      sourceNode = null;
    }
    if (mediaStream) {
      mediaStream.getTracks().forEach((track) => track.stop());
      mediaStream = null;
    }
    if (audioContext) {
      audioContext.close();
      audioContext = null;
    }
  }

  function stopStreaming() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "stop" }));
    }
    cleanupAudio();
  }

  startBtn.addEventListener("click", () => {
    startStreaming().catch((error) => {
      appendLog(`Start failed: ${error.message}`);
      setStatus("Error", true);
    });
  });

  stopBtn.addEventListener("click", stopStreaming);
})();
