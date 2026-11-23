package qrcode

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"strings"
)

// quietZone 静默区模块数(标准要求至少 4)。
const quietZone = 4

// PNG 把内容渲染成 PNG 图片(带静默区)。
//
// scale 为每个模块的像素数,小于 1 时按 1 处理。
func PNG(content string, scale int) ([]byte, error) {
	code, err := Encode(content)
	if err != nil {
		return nil, err
	}
	return code.PNG(scale)
}

// SVG 把内容渲染成 SVG 矢量图。
//
// scale 为每个模块的像素数,便于按需缩放。
func SVG(content string, scale int) ([]byte, error) {
	code, err := Encode(content)
	if err != nil {
		return nil, err
	}
	return code.SVG(scale)
}

// PNG 把二维码渲染成 PNG 图片。
func (c *Code) PNG(scale int) ([]byte, error) {
	if scale < 1 {
		scale = 1
	}
	total := (c.Size + quietZone*2) * scale
	img := image.NewRGBA(image.Rect(0, 0, total, total))
	// 背景白色
	white := color.RGBA{R: 255, G: 255, B: 255, A: 255}
	black := color.RGBA{R: 0, G: 0, B: 0, A: 255}
	for y := 0; y < total; y++ {
		for x := 0; x < total; x++ {
			img.Set(x, y, white)
		}
	}
	// 模块
	for my := 0; my < c.Size; my++ {
		for mx := 0; mx < c.Size; mx++ {
			if !c.modules[my][mx] {
				continue
			}
			for dy := 0; dy < scale; dy++ {
				for dx := 0; dx < scale; dx++ {
					px := (mx+quietZone)*scale + dx
					py := (my+quietZone)*scale + dy
					img.Set(px, py, black)
				}
			}
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, fmt.Errorf("qrcode: encode png: %w", err)
	}
	return buf.Bytes(), nil
}

// SVG 把二维码渲染成 SVG 矢量图。
func (c *Code) SVG(scale int) ([]byte, error) {
	if scale < 1 {
		scale = 1
	}
	total := (c.Size + quietZone*2) * scale
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" shape-rendering="crispEdges">`,
		total, total, total, total))
	sb.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>`)
	sb.WriteString(`<path fill="#000000" d="`)
	for my := 0; my < c.Size; my++ {
		for mx := 0; mx < c.Size; mx++ {
			if !c.modules[my][mx] {
				continue
			}
			x := (mx + quietZone) * scale
			y := (my + quietZone) * scale
			sb.WriteString(fmt.Sprintf("M%d %dh%dv%dh-%dz", x, y, scale, scale, scale))
		}
	}
	sb.WriteString(`"/>`)
	sb.WriteString(`</svg>`)
	return []byte(sb.String()), nil
}
