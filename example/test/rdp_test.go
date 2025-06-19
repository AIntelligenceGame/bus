package test

import (
	"fmt"
	"testing"

	"github.com/icodeface/grdp"
	"github.com/icodeface/grdp/glog"
)

func TestRDP(t *testing.T) {
	client := grdp.NewClient("10.188.60.103:3389", glog.DEBUG)
	err := client.Login("Administrator", "123456")
	if err != nil {
		fmt.Println("----------------------login failed,", err)
	} else {
		fmt.Println("----------------------login success")
	}
}
