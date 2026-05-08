function exportTableToCsv(tableId, filename) {
      const table = document.getElementById(tableId);
      if (!table) return;
      const rows = Array.from(table.querySelectorAll('tr'));
      const csv = rows.map((row) => {
        const cells = Array.from(row.querySelectorAll('th, td'));
        return cells.map((cell) => {
          const text = (cell.innerText || '').replace(/\n/g, ' ').trim();
          const escaped = text.replace(/"/g, '""');
          return `"${escaped}"`;
        }).join(',');
      }).join('\n');
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(link.href);
    }
