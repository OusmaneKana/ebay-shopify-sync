import fs from 'fs/promises';
import { SpreadsheetFile, Workbook } from '@oai/artifact-tool';

const navy = '#17365D';
const white = '#FFFFFF';
const paleBlue = '#D9EAF7';
const paleGreen = '#E2F0D9';
const paleAmber = '#FFF2CC';
const font = { name: 'Arial', size: 10 };

async function makeReport(path, outPath, title) {
  const input = JSON.parse(await fs.readFile(path, 'utf8'));
  const wb = Workbook.create();
  const summary = wb.worksheets.add('Category Summary');
  const detail = wb.worksheets.add('Sold Items');

  summary.getRange('A1:F1').merge();
  summary.getRange('A1').values = [[title]];
  summary.getRange('A1:F1').format = { fill: navy, font: { name: 'Arial', size: 16, bold: true, color: white } };
  summary.getRange('A3:B7').values = [
    ['Snapshot captured', `As of ${input.captured_at}`],
    ['Analysis window', `${input.period_days} days`],
    ['Sold items reviewed', input.items.length],
    ['Categories', input.summary.length],
    ['Ranking', 'Fastest median days to sale first'],
  ];
  summary.getRange('A3:A7').format.font = { name: 'Arial', bold: true };
  summary.getRange('A3:B7').format = { font };
  summary.getRange('A3:A7').format.font = { name: 'Arial', bold: true };

  summary.getRange('A9:F9').values = [['Category', 'Sold Items', 'Median Days to Sale', 'Fastest Sale (Days)', 'Revenue (USD)', 'Fastest Item']];
  summary.getRange('A9:F9').format = { fill: navy, font: { name: 'Arial', bold: true, color: white }, wrapText: true };
  summary.getRange(`A10:F${9 + input.summary.length}`).values = input.summary.map(r => [r.category, r.sold_items, r.median_days_to_sale, r.fastest_days, r.revenue, r.fastest_item]);
  summary.getRange(`A10:F${9 + input.summary.length}`).format = { font, verticalAlignment: 'center' };
  summary.getRange(`C10:D${9 + input.summary.length}`).format.numberFormat = '0.0';
  summary.getRange(`E10:E${9 + input.summary.length}`).format.numberFormat = '$#,##0.00';
  summary.getRange(`A10:F${9 + input.summary.length}`).format.fill = paleBlue;
  if (input.summary.length) summary.getRange('A10:F10').format.fill = paleGreen;

  const noteRow = 12 + input.summary.length;
  summary.getRange(`A${noteRow}:F${noteRow}`).merge();
  summary.getRange(`A${noteRow}`).values = [['Method: items are sorted by days from listing to first recorded sale. Categories come from the saved listing category; missing categories were inferred from title keywords and are included in the detail tab. This is a speed report, not a profitability ranking.']];
  summary.getRange(`A${noteRow}:F${noteRow}`).format = { wrapText: true, verticalAlignment: 'top', font: { name: 'Arial', italic: true, color: '#404040' } };

  const headers = ['Listing ID', 'Title', 'Category', 'Days to Sale', 'Price (USD)', 'Revenue (USD)', 'Units', 'State'];
  detail.getRange(`A1:H${input.items.length + 1}`).values = [headers, ...input.items.map(r => [r.listing_id, r.title, r.category, r.days_to_sale, r.price, r.revenue, r.units, r.state])];
  detail.getRange(`A1:H${input.items.length + 1}`).format = { font, verticalAlignment: 'center' };
  detail.getRange('A1:H1').format = { fill: navy, font: { name: 'Arial', bold: true, color: white }, wrapText: true };
  detail.freezePanes.freezeRows(1);
  const table = detail.tables.add(`A1:H${input.items.length + 1}`, true, `${input.source.replace(/[^A-Za-z]/g, '')}CategorySpeed`);
  table.style = 'TableStyleMedium2';
  table.showFilterButton = true;
  detail.getRange(`D2:D${input.items.length + 1}`).format.numberFormat = '0';
  detail.getRange(`E2:F${input.items.length + 1}`).format.numberFormat = '$#,##0.00';
  detail.getRange(`A1:H${Math.min(input.items.length + 1, 120)}`).format.autofitColumns();
  detail.getRange('B:B').format.columnWidth = 48;
  detail.getRange('C:C').format.columnWidth = 28;
  summary.getRange('A:A').format.columnWidth = 28;
  summary.getRange('B:B').format.columnWidth = 16;
  summary.getRange('C:E').format.columnWidth = 18;
  summary.getRange('F:F').format.columnWidth = 52;

  wb.recalculate();
  const preview = await wb.render({ sheetName: 'Category Summary', autoCrop: 'all', scale: 1, format: 'png' });
  await fs.writeFile(outPath.replace('.xlsx', '-preview.png'), new Uint8Array(await preview.arrayBuffer()));
  const xlsx = await SpreadsheetFile.exportXlsx(wb);
  await xlsx.save(outPath);
  console.log(JSON.stringify({ output: outPath, source: input.source, categories: input.summary.length, items: input.items.length }));
}

await makeReport('/tmp/etsy_category_speed.json', '/Users/administrator/Desktop/Dev/ebay-shopify-sync/outputs/etsy-category-speed/etsy-category-speed.xlsx', 'Etsy Category Sale-Speed Report');
await makeReport('/tmp/ebay_category_speed.json', '/Users/administrator/Desktop/Dev/ebay-shopify-sync/outputs/ebay-category-speed/ebay-category-speed.xlsx', 'eBay Category Sale-Speed Report');
