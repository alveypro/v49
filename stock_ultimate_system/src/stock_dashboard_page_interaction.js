(function() {
      loadPrimaryResultCard();
      const currentView = __CURRENT_VIEW__;
      const candidateIndex = __CANDIDATE_INDEX__;
      const candidateCount = __CANDIDATE_COUNT__;
      const candidateBaseHref = __CANDIDATE_BASE_HREF__;
      const copyButton = document.querySelector('[data-copy-link]');
      const modeButton = document.querySelector('[data-toggle-mode]');
      if (copyButton) {
        copyButton.addEventListener('click', async function() {
          const href = copyButton.getAttribute('data-copy-link');
          const absoluteUrl = new URL(href, window.location.origin).toString();
          try {
            await navigator.clipboard.writeText(absoluteUrl);
            copyButton.textContent = '已复制链接';
            window.setTimeout(() => {
              copyButton.textContent = '复制当前链接';
            }, 1200);
          } catch (error) {
            window.prompt('复制当前链接', absoluteUrl);
          }
        });
      }
      if (modeButton) {
        const modeKey = modeButton.getAttribute('data-toggle-mode');
        const storageKey = `airivo-ui-${modeKey}`;
        const enabled = window.localStorage.getItem(storageKey) === '1';
        if (enabled) {
          document.body.classList.add(modeKey);
          modeButton.classList.add('tool-btn-active');
        }
        modeButton.addEventListener('click', function() {
          const nextEnabled = !document.body.classList.contains(modeKey);
          document.body.classList.toggle(modeKey, nextEnabled);
          modeButton.classList.toggle('tool-btn-active', nextEnabled);
          window.localStorage.setItem(storageKey, nextEnabled ? '1' : '0');
        });
      }
      if (currentView !== 'candidates' || candidateCount <= 1) return;
      document.addEventListener('keydown', function(event) {
        if (event.metaKey || event.ctrlKey || event.altKey) return;
        if (/^[1-9]$/.test(event.key)) {
          const targetIndex = Number(event.key) - 1;
          if (targetIndex < candidateCount) {
            window.location.href = candidateBaseHref.replace('candidate=0', 'candidate=' + targetIndex);
            return;
          }
        }
        if (event.key === 'ArrowLeft' && candidateIndex > 0) {
          window.location.href = candidateBaseHref.replace('candidate=0', 'candidate=' + (candidateIndex - 1));
        }
        if (event.key === 'ArrowRight' && candidateIndex < candidateCount - 1) {
          window.location.href = candidateBaseHref.replace('candidate=0', 'candidate=' + (candidateIndex + 1));
        }
      });
    })();
